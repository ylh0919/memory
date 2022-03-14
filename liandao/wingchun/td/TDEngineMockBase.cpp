#include "TDEngineMockBase.h"
#include "longfist/LFUtils.h"
#include "TypeConvert.hpp"
#include <boost/algorithm/string.hpp>

#include <writer.h>
#include <stringbuffer.h>
#include <document.h>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <cpr/cpr.h>
#include <chrono>
#include <limits>

using cpr::Delete;
using cpr::Get;
using cpr::Url;
using cpr::Body;
using cpr::Header;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;
using cpr::Timeout;

using rapidjson::StringRef;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using rapidjson::Document;
using rapidjson::Value;
using std::string;
using std::to_string;

using namespace std;
mutex orderbook_lock;
const int64_t max_number = std::numeric_limits<int64_t>::max();
const int64_t min_number = std::numeric_limits<int64_t>::min();
USING_WC_NAMESPACE

TDEngineMockBase::TDEngineMockBase(std::string exchange_name,int exchange_id): ITDEngine(exchange_id)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine."+exchange_name);
    KF_LOG_INFO(logger, "[TDEngineMockBase]");
    module_name ="TD" + exchange_name;
    QRY_POS_MOCK = exchange_id*1000+ 201;
    ORDER_MOCK = exchange_id*1000+ 204;
    ORDER_ACTION_MOCK = exchange_id*1000+ 207;
}
TDEngineMockBase::~TDEngineMockBase()
{
}

void TDEngineMockBase::init()
{
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineMockBase::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineMockBase::resize_accounts(int account_num)
{
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineMockBase::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    
    trade_latency_ns = j_config["trade_latency_ns"].get<int>();
    cancel_latency_ns = j_config["cancel_latency_ns"].get<int>();
    source_id_105 = j_config["source_id_105"].get<int>();
 if (j_config.find("history_data_path") != j_config.end())
    {
        history_data_path = j_config["history_data_path"].get<string>();
    }

    if (j_config.find("history_date_type") != j_config.end())
    {
        history_date_type = j_config["history_date_type"].get<int>();
    }
    KF_LOG_DEBUG(logger, "history_date_type: " << history_date_type);
    if (j_config.find("period_millisec") != j_config.end())
    {
        period_millisec = j_config["period_millisec"].get<int>();
    }

    AccountUnitMock& unit = account_units[idx];
    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();

    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    //display usage:
    if(unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().size() == 0) {
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    //cancel all openning orders on TD startup
    init_orderbook(unit);
    // set up
    TradeAccount account = {};
    return account;
}

void TDEngineMockBase::init_orderbook(AccountUnitMock unit)
{
    KF_LOG_INFO(logger, "[init_orderbook]");   
    for(auto it = unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); it != unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); it++){
        OrderBook orderbook;
        orderbook_lock.lock();
        orderbook_map.insert(make_pair(it->first, orderbook));
        orderbook_lock.unlock();
    }

}

void TDEngineMockBase::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");   
}


void TDEngineMockBase::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[login]");
    connect(timeout_nsec);
}

void TDEngineMockBase::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineMockBase::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineMockBase::is_logged_in() const
{
    KF_LOG_INFO(logger, "[is_logged_in]");  
    return true;
}

bool TDEngineMockBase::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}

/**
 * req functions
 */
void TDEngineMockBase::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitMock& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position] (InstrumentID) " << data->InstrumentID);

    int errorId = 0;
    std::string errorMsg = "";

    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, QRY_POS_MOCK, 1, requestId);

    LFRspPositionField pos;
    memset(&pos, 0, sizeof(LFRspPositionField));
    strncpy(pos.BrokerID, data->BrokerID, 11);
    strncpy(pos.InvestorID, data->InvestorID, 19);
    strncpy(pos.InstrumentID, data->InstrumentID, 31);
    pos.PosiDirection = LF_CHAR_Long;
    pos.HedgeFlag = LF_CHAR_Speculation;
    pos.Position = 0;
    pos.YdPosition = 0;
    pos.PositionCost = 0;

    KF_LOG_INFO(logger, "[req_investor_position] (!findSymbolInResult) (requestId)" << requestId);
    on_rsp_position(&pos, 1, requestId, errorId, errorMsg.c_str());

}

void TDEngineMockBase::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}



void TDEngineMockBase::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitMock& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_insert999]" << " (rid)" << requestId
                                              << " (Tid)" << data->InstrumentID
                                              << "(OrderPriceType)" << data->OrderPriceType
                                              << "(Direction)" << data->Direction
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, ORDER_MOCK, 1/*ISLAST*/, requestId);

    int errorId = 0;
    std::string errorMsg = "";

    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList, ignore it  (rid)" << requestId <<
                                                                                      " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, ORDER_MOCK, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);

    // modified for post_only
    //KF_LOG_DEBUG(logger, "[req_order_insert] (data->MiscInfo[43])" << data->MiscInfo[43] << ", (data->MiscInfo)" << data->MiscInfo);
    bool post_only_reject = false;
    if (is_post_only(data)) {
        orderbook_lock.lock();
        auto it = orderbook_map.find(std::string(data->InstrumentID));
        int64_t data_price = data->LimitPrice; // 限价单的价格
        if (it != orderbook_map.end()) {
            if (data->Direction == LF_CHAR_Buy && !it->second.orderbook_sell_line.empty()) {
                auto one_order = it->second.orderbook_sell_line.begin(); // map<int64_t,map<int64_t,LFRtnOrderField> >
                int64_t sell_price = one_order->first;
                if (sell_price < data_price) {
                    errorId = 200;
                    errorMsg = "the price cannot be postOnly, data->Direction==LF_CHAR_Buy, sell_price =" + to_string(sell_price) + ", data_price=" + to_string(data_price);
                    KF_LOG_ERROR(logger, "[req_order_insert]: found error while checking is_postOnly" << requestId <<
                        " (errorId)" << errorId << " (errorMsg) " << errorMsg);
                    on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
                    raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, ORDER_MOCK, 1, requestId, errorId, errorMsg.c_str());
                    post_only_reject = true;
                    //return;
                }

            }

            else if (data->Direction == LF_CHAR_Sell && !it->second.orderbook_buy_line.empty()) {
                auto one_order = it->second.orderbook_buy_line.begin(); // map<int64_t,map<int64_t,LFRtnOrderField> >
                int64_t buy_price = one_order->first;
                if (buy_price > data_price) {
                    errorId = 200;
                    errorMsg = "the price cannot be postOnly, data->Direction==LF_CHAR_Sell, buy_price =" + to_string(buy_price) + ", data_price=" + to_string(data_price);
                    KF_LOG_ERROR(logger, "[req_order_insert]: found error while checking is_postOnly" << requestId <<
                        " (errorId)" << errorId << " (errorMsg) " << errorMsg);
                    on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
                    raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, ORDER_MOCK, 1, requestId, errorId, errorMsg.c_str());
                    post_only_reject = true;
                    //return;
                }
            }
        }
        orderbook_lock.unlock();
    }
    if (!post_only_reject) {
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, ORDER_MOCK, 1, requestId, errorId, errorMsg.c_str());
    };
    // modified

    // modified begin : fak or ioc
    LFRtnOrderField rtn_order_cancel;
    // modified end

    orderbook_lock.lock();
    auto it = orderbook_map.find(std::string(data->InstrumentID));
    if(it != orderbook_map.end()){
    
        int64_t data_price = data->LimitPrice; // 限价单的价格
        KF_LOG_DEBUG(logger,"listening:req_order_insert 0 LimitPrice is %ld"<<  data->LimitPrice);
        if(data->OrderPriceType == LF_CHAR_AnyPrice) // 市价单
        {
            if(data->Direction == LF_CHAR_Sell) data_price = 0;
            else if(data->Direction == LF_CHAR_Buy) data_price = max_number;
            KF_LOG_DEBUG(logger, "(ordertype)Anyprice (price)"<<data_price);
        }
        
        // first send nottouched
        LFRtnOrderField rtn_order_A;
        memset(&rtn_order_A, 0, sizeof(LFRtnOrderField));
        strcpy(rtn_order_A.BrokerID,data->BrokerID);
        strcpy(rtn_order_A.UserID,data->UserID);
        strcpy(rtn_order_A.InvestorID,data->InvestorID);
        strcpy(rtn_order_A.BusinessUnit,data->BusinessUnit);
        strcpy(rtn_order_A.InstrumentID, data->InstrumentID);
        strcpy(rtn_order_A.OrderRef,data->OrderRef);
        strcpy(rtn_order_A.ExchangeID,exchange_name.c_str());
        rtn_order_A.LimitPrice = data_price;
        KF_LOG_DEBUG(logger,"listening:req_order_insert 10 LimitPrice is %ld"<<  data_price);
        rtn_order_A.VolumeTraded = 0;
        rtn_order_A.VolumeTotal = rtn_order_A.VolumeTotalOriginal = data->Volume;
        // modified here
        if (data->TimeCondition == LF_CHAR_FAK) {
            rtn_order_A.TimeCondition = LF_CHAR_IOC;
            KF_LOG_DEBUG(logger, "[req_order_insert] data->TimeCondition == LF_CHAR_FAK, FAK to IOC");
        }
        else
            rtn_order_A.TimeCondition = data->TimeCondition;
        // end
        rtn_order_A.VolumeCondition = data->VolumeCondition;
        rtn_order_A.OrderPriceType = data->OrderPriceType;
        rtn_order_A.Direction = data->Direction;
        rtn_order_A.OffsetFlag = data->OffsetFlag;
        rtn_order_A.HedgeFlag = data->HedgeFlag;
        rtn_order_A.OrderStatus = LF_CHAR_NotTouched;
        rtn_order_A.RequestID = requestId;
        std::this_thread::sleep_for(std::chrono::nanoseconds(trade_latency_ns));

        // modified for post_only
        if (post_only_reject) {
            rtn_order_A.OrderStatus = LF_CHAR_Canceled;
            on_rtn_order(&rtn_order_A);
            return;
        }

        on_rtn_order(&rtn_order_A);
        // modified here 
        rtn_order_cancel = rtn_order_A;

        LFRtnTradeField rtn_trade_A;
        memset(&rtn_trade_A, 0, sizeof(LFRtnTradeField));
        strcpy(rtn_trade_A.BrokerID,data->BrokerID);
        strcpy(rtn_trade_A.UserID,data->UserID);
        strcpy(rtn_trade_A.InvestorID,data->InvestorID);
        strcpy(rtn_trade_A.BusinessUnit,data->BusinessUnit);
        strcpy(rtn_trade_A.InstrumentID, data->InstrumentID);
        strcpy(rtn_trade_A.OrderRef,data->OrderRef);
        strcpy(rtn_trade_A.ExchangeID,exchange_name.c_str());
        strcpy(rtn_trade_A.TradeID,getTimestampString().c_str());
        strcpy(rtn_trade_A.OrderSysID,data->OrderRef);
        rtn_trade_A.Direction = data->Direction;
        rtn_trade_A.OffsetFlag = data->OffsetFlag;
        rtn_trade_A.HedgeFlag = data->HedgeFlag;

        LFRtnOrderField rtn_order_B;
        memset(&rtn_order_B, 0, sizeof(LFRtnOrderField));
        LFRtnTradeField rtn_trade_B;
        memset(&rtn_trade_B, 0, sizeof(LFRtnTradeField));

        vector<LFRtnOrderField> rtn_orders;
        vector<LFRtnTradeField> rtn_trades;

        map<string,int64_t>::iterator iter;
        uint64_t new_volume = data->Volume;

        // judge whether a deal can be made and send rtn message
        if(data->Direction == LF_CHAR_Buy)
        {
            while(new_volume > 0)
            {
                
                if(it->second.orderbook_sell_line.empty()) 
                {
                    break;
                }
                auto one_order = it->second.orderbook_sell_line.begin(); // map<int64_t,map<int64_t,LFRtnOrderField> >
                int64_t sell_price = one_order->first; 
                if(sell_price > data_price) 
                {
                    break;
                }
                auto info_map = one_order->second.begin(); // map<int64_t,LFRtnOrderField>
                int64_t sell_rcv_time = info_map->first;
                rtn_order_B  = info_map->second;
                one_order->second.erase(info_map);
                if(one_order->second.size() == 0)
                {
                    it->second.orderbook_sell_line.erase(one_order);
                }
                if(rtn_order_B.RequestID != -1)
                {
                    iter = orderref_price_map.find(rtn_order_B.OrderRef);
                    if(iter != orderref_price_map.end()) orderref_price_map.erase(iter);
                }
                

                // uint64_t sell_volume = rtn_order_B.VolumeTotalOriginal;
                uint64_t sell_volume = rtn_order_B.VolumeTotal;

                uint64_t volume_traded = std::min(new_volume,sell_volume);
                int64_t price_traded = sell_price;
                // 成交量 volume_traded 价格 price_traded
                // 买家回报 + 修改data
                rtn_order_A.LimitPrice = data_price;
                KF_LOG_DEBUG(logger,"listening:req_order_insert 11 LimitPrice is %ld"<<  price_traded);
                rtn_order_A.VolumeTraded = volume_traded;
                rtn_order_A.VolumeTotal = new_volume - volume_traded;
                // rtn_order_A.VolumeTotalOriginal = new_volume; 总量不变
                if(new_volume == volume_traded)   rtn_order_A.OrderStatus = LF_CHAR_AllTraded;
                else    rtn_order_A.OrderStatus = LF_CHAR_PartTradedQueueing;          
                rtn_orders.push_back(rtn_order_A);

                rtn_trade_A.Price = price_traded;
                rtn_trade_A.Volume = volume_traded;
                strcpy(rtn_trade_A.TradeTime,getTimestampString().c_str());
                rtn_trades.push_back(rtn_trade_A);

                new_volume = new_volume - volume_traded; 
                // 卖家回报 + 修改orderbook_sell_line(再insert)
                KF_LOG_DEBUG(logger,"req_order_insert : LimitPrice is %ld"<<price_traded);
                rtn_order_B.LimitPrice = data_price;
                rtn_order_B.VolumeTraded = volume_traded;
                // rtn_order_B.VolumeTotal = rtn_order_B.VolumeTotalOriginal - volume_traded;
                rtn_order_B.VolumeTotal = rtn_order_B.VolumeTotal - volume_traded;
                if(rtn_order_B.VolumeTotal == 0) rtn_order_B.OrderStatus = LF_CHAR_AllTraded;
                else rtn_order_B.OrderStatus = LF_CHAR_PartTradedQueueing;

                if(rtn_order_B.RequestID !=-1)
                {
                    rtn_orders.push_back(rtn_order_B);
                    memset(&rtn_trade_B, 0, sizeof(LFRtnTradeField));
                    strcpy(rtn_trade_B.BrokerID,rtn_order_B.BrokerID);
                    strcpy(rtn_trade_B.UserID,rtn_order_B.UserID);
                    strcpy(rtn_trade_B.InvestorID,rtn_order_B.InvestorID);
                    strcpy(rtn_trade_B.BusinessUnit,rtn_order_B.BusinessUnit);
                    strcpy(rtn_trade_B.InstrumentID, rtn_order_B.InstrumentID);
                    strcpy(rtn_trade_B.OrderRef,rtn_order_B.OrderRef);
                    strcpy(rtn_trade_B.ExchangeID,exchange_name.c_str());
                    strcpy(rtn_trade_B.TradeID,getTimestampString().c_str());
                    strcpy(rtn_trade_B.OrderSysID,rtn_order_B.OrderRef);
                    rtn_trade_B.Direction = rtn_order_B.Direction;
                    rtn_trade_B.OffsetFlag = rtn_order_B.OffsetFlag;
                    rtn_trade_B.HedgeFlag = rtn_order_B.HedgeFlag;
                    rtn_trade_B.Price = price_traded;
                    rtn_trade_B.Volume = volume_traded;
                    strcpy(rtn_trade_B.TradeTime,getTimestampString().c_str());
                    rtn_trades.push_back(rtn_trade_B);
                }

                if(rtn_order_B.VolumeTotal==0) continue;
                rtn_order_B.LimitPrice = data_price;
                KF_LOG_DEBUG(logger,"req_order_insert :2 LimitPrice is %ld"<<sell_price);
                rtn_order_B.VolumeTraded = 0;
                // rtn_order_B.VolumeTotalOriginal = rtn_order_B.VolumeTotal;
                
                it->second.orderbook_sell_line.insert(make_pair(sell_price,map<int64_t,LFRtnOrderField>()));
                it->second.orderbook_sell_line[sell_price].insert(make_pair(sell_rcv_time,rtn_order_B));
                if(rtn_order_B.RequestID != -1)
                    orderref_price_map.insert(make_pair(rtn_order_B.OrderRef,sell_price));
                
            }
            // move the left deal to orderbook 
            // modified here : not FAK or IOC
            if(new_volume > 0 && (data->TimeCondition != LF_CHAR_FAK && data->TimeCondition != LF_CHAR_IOC))
            //if(new_volume > 0)
            // mofified end
            {
                rtn_order_A.VolumeTraded = 0;
                rtn_order_A.VolumeTotal = new_volume;
                // rtn_order_A.VolumeTotalOriginal = new_volume;
                rtn_order_A.LimitPrice = data_price;
                KF_LOG_DEBUG(logger,"req_order_insert :3 LimitPrice is %ld"<<data_price);
                KF_LOG_DEBUG(logger, "ZYY DEBUG: move the left buy deal to orderbook,VolumeTotal:"<<rtn_order_A.VolumeTotal<<" VolumeOriginal:"<<rtn_order_A.VolumeTotalOriginal);
                
                it->second.orderbook_buy_line.insert(make_pair(data_price,map<int64_t,LFRtnOrderField>()));
                it->second.orderbook_buy_line[data_price].insert(make_pair(rcv_time,rtn_order_A));
                orderref_price_map.insert(make_pair(rtn_order_A.OrderRef,data_price));
                
            }
        }
        else if(data->Direction == LF_CHAR_Sell)
        {
            while(new_volume > 0)
            {
                
                if(it->second.orderbook_buy_line.empty()) 
                {
                   
                    break;
                }
                auto one_order = it->second.orderbook_buy_line.begin(); // map<int64_t,map<int64_t,LFRtnOrderField> >
                int64_t buy_price = one_order->first; 
                if(buy_price < data_price) 
                {
                    
                    break;
                }
                auto info_map = one_order->second.begin(); // map<int64_t,LFRtnOrderField>
                int64_t buy_rcv_time = info_map->first;
                rtn_order_B  = info_map->second;
                one_order->second.erase(info_map);
                if(one_order->second.size() == 0)
                {
                    KF_LOG_DEBUG(logger, "del in the buy_line");
                    it->second.orderbook_buy_line.erase(one_order);
                }
                if(rtn_order_B.RequestID != -1)
                {
                    iter = orderref_price_map.find(rtn_order_B.OrderRef);
                    if(iter != orderref_price_map.end()) orderref_price_map.erase(iter);
                }
                

                // uint64_t buy_volume = rtn_order_B.VolumeTotalOriginal;
                uint64_t buy_volume = rtn_order_B.VolumeTotal;
                KF_LOG_DEBUG(logger , "can make a deal");
                uint64_t volume_traded = std::min(new_volume,buy_volume);
                // 市价单 成交价格 = buy_price
                // 限价单 成交价格 = data_price
                int64_t price_traded;
                price_traded = buy_price; 
                KF_LOG_DEBUG(logger, "zyy Debug [req_order_insert](price_traded)"<<price_traded<<"(volume_traded)"<<volume_traded);
                // 成交量 volume_traded 价格 price_traded
                // 卖家A回报 + 修改data
                rtn_order_A.LimitPrice = data_price;
                KF_LOG_DEBUG(logger,"req_order_insert :4 LimitPrice is %ld"<<price_traded);
                rtn_order_A.VolumeTraded = volume_traded;
                rtn_order_A.VolumeTotal = new_volume - volume_traded;
                // rtn_order_A.VolumeTotalOriginal = new_volume;
                if(new_volume == volume_traded)   rtn_order_A.OrderStatus = LF_CHAR_AllTraded;
                else    rtn_order_A.OrderStatus = LF_CHAR_PartTradedQueueing;          
                rtn_orders.push_back(rtn_order_A);

                rtn_trade_A.Price = price_traded;
                rtn_trade_A.Volume = volume_traded;
                strcpy(rtn_trade_A.TradeTime,getTimestampString().c_str());
                rtn_trades.push_back(rtn_trade_A);

                new_volume -= volume_traded; 
                // 买家B回报 + 修改orderbook_buy_line(再insert)
                rtn_order_B.LimitPrice = data_price;
                KF_LOG_DEBUG(logger,"req_order_insert :5 LimitPrice is %ld"<<price_traded);
                rtn_order_B.VolumeTraded = volume_traded;
                // rtn_order_B.VolumeTotal = rtn_order_B.VolumeTotalOriginal - volume_traded;
                rtn_order_B.VolumeTotal = rtn_order_B.VolumeTotal - volume_traded;
                if(rtn_order_B.VolumeTotal == 0) rtn_order_B.OrderStatus = LF_CHAR_AllTraded;
                else rtn_order_B.OrderStatus = LF_CHAR_PartTradedQueueing;
                
                if(rtn_order_B.RequestID != -1)
                {
                    rtn_orders.push_back(rtn_order_B);
                    memset(&rtn_trade_B, 0, sizeof(LFRtnTradeField));
                    strcpy(rtn_trade_B.BrokerID,rtn_order_B.BrokerID);
                    strcpy(rtn_trade_B.UserID,rtn_order_B.UserID);
                    strcpy(rtn_trade_B.InvestorID,rtn_order_B.InvestorID);
                    strcpy(rtn_trade_B.BusinessUnit,rtn_order_B.BusinessUnit);
                    strcpy(rtn_trade_B.InstrumentID, rtn_order_B.InstrumentID);
                    strcpy(rtn_trade_B.OrderRef,rtn_order_B.OrderRef);
                    strcpy(rtn_trade_B.ExchangeID,exchange_name.c_str());
                    strcpy(rtn_trade_B.TradeID,getTimestampString().c_str());
                    strcpy(rtn_trade_B.OrderSysID,rtn_order_B.OrderRef);
                    rtn_trade_B.Direction = rtn_order_B.Direction;
                    rtn_trade_B.OffsetFlag = rtn_order_B.OffsetFlag;
                    rtn_trade_B.HedgeFlag = rtn_order_B.HedgeFlag;
                    rtn_trade_B.Price = price_traded;
                    rtn_trade_B.Volume = volume_traded;
                    strcpy(rtn_trade_B.TradeTime,getTimestampString().c_str());
                    rtn_trades.push_back(rtn_trade_B);
                }

                if(rtn_order_B.VolumeTotal == 0) continue;
                rtn_order_B.LimitPrice = data_price;
                KF_LOG_DEBUG(logger,"req_order_insert 6 LimitPrice is %ld"<<buy_price);
                rtn_order_B.VolumeTraded = 0;
                // rtn_order_B.VolumeTotalOriginal = rtn_order_B.VolumeTotal;
                
                it->second.orderbook_buy_line.insert(make_pair(buy_price,map<int64_t,LFRtnOrderField>()));
                it->second.orderbook_buy_line[buy_price].insert(make_pair(buy_rcv_time,rtn_order_B));
                if(rtn_order_B.RequestID != -1)
                    orderref_price_map.insert(make_pair(rtn_order_B.OrderRef,buy_price));
                
            }
            // move the left deal to orderbook 
            // modified here : not FAK or IOC
            if (new_volume > 0 && (data->TimeCondition != LF_CHAR_FAK && data->TimeCondition != LF_CHAR_IOC))
            //if(new_volume > 0)
            // mofified end
            {
                rtn_order_A.VolumeTraded = 0;
                rtn_order_A.VolumeTotal = new_volume;
                // rtn_order_A.VolumeTotalOriginal = new_volume;
                rtn_order_A.LimitPrice = data_price;
                KF_LOG_DEBUG(logger,"req_order_insert 7 LimitPrice is %ld"<<data_price);
                KF_LOG_DEBUG(logger, "ZYY DEBUG: move the left sell deal to orderbook,VolumeTotal:"<<rtn_order_A.VolumeTotal<<" VolumeOriginal:"<<rtn_order_A.VolumeTotalOriginal);
                
                it->second.orderbook_sell_line.insert(make_pair(data_price,map<int64_t,LFRtnOrderField>()));
                it->second.orderbook_sell_line[data_price].insert(make_pair(rcv_time,rtn_order_A));
                orderref_price_map.insert(make_pair(rtn_order_A.OrderRef,data_price));
                
            }
        }
        write_orderbook(data->InstrumentID,data->ExchangeID,rcv_time,it);

        // modified here : if FAK or IOC
        if (data->TimeCondition == LF_CHAR_FAK || data->TimeCondition == LF_CHAR_IOC) {
            KF_LOG_INFO(logger, "data->TimeCondition == FAK or IOC, prepare to cancel order");
            if (rtn_orders.size() == 0) {
                KF_LOG_INFO(logger, "rtn_orders.size() == 0, add rtn_order_cancel to rtn_orders");
                rtn_order_cancel.OrderStatus = LF_CHAR_Canceled;
                rtn_orders.push_back(rtn_order_cancel);
            }
            else if (rtn_orders.back().OrderStatus == LF_CHAR_AllTraded || rtn_orders.back().OrderStatus == LF_CHAR_Canceled
                || rtn_orders.back().OrderStatus == LF_CHAR_Error || rtn_orders.back().OrderStatus == LF_CHAR_NoTradeNotQueueing) {
                KF_LOG_INFO(logger, "OrderStatus == alltraded || canceled || error || noqueueing, no need to cancel order");
            }
            else 
            {
                KF_LOG_INFO(logger, "rtn_orders.size() > 0, set rtn_orders.back().OrderStatus = LF_CHAR_Canceled ");
                rtn_orders.back().OrderStatus = LF_CHAR_Canceled;
            }
        }
        // modified end

        // wait a moment and send rtn
        for(int i=0; i<rtn_orders.size(); i++ )
        {
            on_rtn_order(&rtn_orders[i]);
        }
        for(int i=0; i<rtn_trades.size(); i++)
        {
            on_rtn_trade(&rtn_trades[i]);
        }
    }
    orderbook_lock.unlock();
}

void TDEngineMockBase::parse(const LFBatchCancelOrderField* data, LFOrderActionField* res, int index) {
    memset(res, 0, sizeof(LFOrderActionField));
    strncpy(res->BrokerID, data->BrokerID, sizeof(res->BrokerID));
    strncpy(res->InvestorID, data->InvestorID, sizeof(res->InvestorID));
    strncpy(res->InstrumentID, data->InstrumentID, sizeof(res->InstrumentID));
    strncpy(res->ExchangeID, data->ExchangeID, sizeof(res->ExchangeID));
    strncpy(res->UserID, data->UserID, sizeof(res->UserID));
    res->RequestID = data->RequestID;

    res->KfOrderID = data->InfoList[index].KfOrderID;
    res->ActionFlag = data->InfoList[index].ActionFlag;
    strncpy(res->OrderRef, data->InfoList[index].OrderRef, sizeof(res->OrderRef));
    strncpy(res->OrderSysID, data->InfoList[index].OrderSysID, sizeof(res->OrderSysID));
    strncpy(res->MiscInfo, data->InfoList[index].MiscInfo, sizeof(res->MiscInfo));
}

void TDEngineMockBase::req_batch_cancel_orders(const LFBatchCancelOrderField* data, int account_index, int requestId, long rcv_time) {
    LFOrderActionField* tmp = new LFOrderActionField;
    for (int i = 0; i < data->SizeOfList; i++) {
        parse(data, tmp, i);
        req_order_action(tmp, account_index, requestId, rcv_time);
    }
    free(tmp);
}

void TDEngineMockBase::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitMock& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, ORDER_ACTION_MOCK, 1, requestId);
    int errorId = 0;
    std::string errorMsg = "";

    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, ORDER_ACTION_MOCK, 1, requestId, errorId, errorMsg.c_str());
        return;
    }

    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);


    if(errorId != 0)
    {
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
    }
    raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, ORDER_ACTION_MOCK, 1, requestId, errorId, errorMsg.c_str());

    orderbook_lock.lock();//hbdm
    auto it = orderbook_map.find(std::string(data->InstrumentID));
    if(it != orderbook_map.end()){

        // 判断line里是否存在该订单
        bool if_exists = false;
        LfDirectionType cancel_order_direction;
        int64_t cancel_order_price;
        map<string,int64_t>::iterator iter;
        LFRtnOrderField tmp,rtn_order;
        iter = orderref_price_map.find(data->OrderRef);
        if(iter != orderref_price_map.end())
        {
            KF_LOG_DEBUG(logger, "[req_order_action] find in the orderbook");
            cancel_order_price = iter->second;
            
            auto iter1 = it->second.orderbook_buy_line.find(cancel_order_price); // map<int64_t,map<int64_t,LFRtnOrderField> >
            if(iter1 != it->second.orderbook_buy_line.end())
            {
                auto iter2 = iter1->second.begin();
                for( ; iter2!=iter1->second.end() ; iter2++)
                {
                    tmp = iter2->second;
                    KF_LOG_DEBUG(logger, "orderbook_buy_line orderref = " << tmp.OrderRef);
                    if(strcmp(tmp.OrderRef,data->OrderRef)==0) // find
                    {
                    KF_LOG_DEBUG(logger, "[req_order_action] find in the orderbook_buy_line");
                        if_exists = true;
                        cancel_order_direction = LF_CHAR_Buy;
                        break;
                    }
                }
            }
            iter1 = it->second.orderbook_sell_line.find(cancel_order_price);
            if(!if_exists && iter1 != it->second.orderbook_sell_line.end())
            {
                auto iter2 = iter1->second.begin();
                for( ; iter2!=iter1->second.end() ; iter2++)
                {
                    tmp = iter2->second;
                    KF_LOG_DEBUG(logger, "orderbook_sell_line orderref = " << tmp.OrderRef);
                    if(strcmp(tmp.OrderRef,data->OrderRef)==0) // find
                    {
                        KF_LOG_DEBUG(logger, "[req_order_action] find in the orderbook_sell_line");
                        if_exists = true;
                        cancel_order_direction = LF_CHAR_Sell;
                        break;
                    }
                }
            }
            
        }  
        // 不存在，直接报错
        if(!if_exists)
        {
            KF_LOG_DEBUG(logger, "[req_order_action] the order do not exist");
            int errorId = 101;
            std::string errorMsg = std::string(data->OrderRef) + ", the order is closed.";
            on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
            orderbook_lock.unlock();
            return;
        }
        // 存在，等待cancel_latency后再判断
        // this_thread::sleep_for(chrono::nanoseconds(cancel_latency_ns));
        if_exists = false;
        if(cancel_order_direction == LF_CHAR_Buy) // buy line中撤销
        {
            
            auto iter1 = it->second.orderbook_buy_line.find(cancel_order_price); // map<int64_t,map<int64_t,LFRtnOrderField> >
            if(iter1 != it->second.orderbook_buy_line.end())
            {
                auto iter2 = iter1->second.begin();
                for( ; iter2!=iter1->second.end() ; iter2++)
                {
                    tmp = iter2->second;
                    if(strcmp(tmp.OrderRef,data->OrderRef)==0) // find
                    {
                        if_exists = true;
                        iter1->second.erase(iter2);
                        iter = orderref_price_map.find(data->OrderRef);
                        if(iter != orderref_price_map.end()) orderref_price_map.erase(iter);
                        if(iter1->second.size() < 1)  it->second.orderbook_buy_line.erase(iter1);
                        break;
                    }
                }
            }
            
        }
        else // sell line中撤销
        {
            
            auto iter1 = it->second.orderbook_sell_line.find(cancel_order_price); // map<int64_t,map<int64_t,LFRtnOrderField> >
            if(iter1 != it->second.orderbook_sell_line.end())
            {
                auto iter2 = iter1->second.begin();
                for( ; iter2!=iter1->second.end() ; iter2++)
                {
                    tmp = iter2->second;
                    if(strcmp(tmp.OrderRef,data->OrderRef)==0) // find
                    {
                        if_exists = true;
                        iter1->second.erase(iter2);
                        iter = orderref_price_map.find(data->OrderRef);
                        if(iter != orderref_price_map.end()) orderref_price_map.erase(iter);
                        if(iter1->second.size() < 1) it->second.orderbook_sell_line.erase(iter1);
                        break;
                    }
                }
            }
            
        }
        write_orderbook(data->InstrumentID,data->ExchangeID,rcv_time,it);   
        if(!if_exists)
        {
            KF_LOG_DEBUG(logger, "[req_order_action] the order do not exist again");
            int errorId = 101;
            std::string errorMsg = std::string(data->OrderRef) + ", the order is closed.";
            on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        }
        else
        {
            rtn_order = tmp;
            rtn_order.OrderStatus = LF_CHAR_Canceled;
            this_thread::sleep_for(chrono::nanoseconds(trade_latency_ns));
            on_rtn_order(&rtn_order);
        } 

    }
    orderbook_lock.unlock();
}


void TDEngineMockBase::set_reader_thread()
{
    ITDEngine::set_reader_thread();
    KF_LOG_INFO(logger, "TDEngineMockBase::set_reader_thread" );
    rest_thread = std::shared_ptr<std::thread>(new std::thread(boost::bind(&TDEngineMockBase::listening2, this)));
}


void TDEngineMockBase::updateOrderBook106(LfDirectionType direction,int64_t price, uint64_t&  volume, std::map<std::string, OrderBook>::iterator orderbook_it)
{

    KF_LOG_INFO(logger, "[TDEngineMockBase:: updateOrderBook] (direction)" << direction <<" (price)"<< price << " (volume)" << volume);
    int64_t buy_price,sell_price;
    if(orderbook_it->second.orderbook_buy_line.empty()) buy_price = -1;
    else buy_price = orderbook_it->second.orderbook_buy_line.begin()->first;
    if(orderbook_it->second.orderbook_sell_line.empty()) sell_price = -1;
    else sell_price = orderbook_it->second.orderbook_sell_line.begin()->first;
    vector<LFRtnOrderField> rtn_orders;
    vector<LFRtnTradeField> rtn_trades;
    LFRtnOrderField rtn_order;
    LFRtnTradeField rtn_trade;
    map<int64_t,map<int64_t,LFRtnOrderField> >::iterator iter1;
    map<int64_t,LFRtnOrderField>::iterator iter2;
    map<string,int64_t>::iterator iter;
    uint64_t volume_traded;
    int64_t price_traded;
    if(direction == LF_CHAR_Sell)
    {
        if(price<=buy_price)
        {
            for(iter1 = orderbook_it->second.orderbook_buy_line.begin(); iter1 != orderbook_it->second.orderbook_buy_line.end(); )
            {
                buy_price = iter1->first;
                if(price > buy_price) break;
                if(volume <= 0) break;
                for(iter2 = iter1->second.begin(); iter2 != iter1->second.end(); )
                {
                    if(volume <= 0) break;
                    int64_t buy_rcv_time = iter2->first;
                    rtn_order = iter2->second;
                    if(rtn_order.RequestID == -1) // order from 106 
                    {

                        if(volume <= rtn_order.VolumeTotal)
                        {
                            volume =0; // 消耗完106的，但不消耗本地的
                        } 
                        else
                        {

                            volume -= rtn_order.VolumeTotal; 
                        }   
                        iter2++;                            
                    }
                    else // order from local
                    {

                        volume_traded = std::min(volume,rtn_order.VolumeTotal);
                        price_traded = rtn_order.LimitPrice;
                        // 卖家回报 + 修改md
                        volume-= volume_traded;
                        // 买家回报 + 修改orderbook_buy_line
                        rtn_order.VolumeTraded = volume_traded;
                        // rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - volume_traded;
                        rtn_order.VolumeTotal = rtn_order.VolumeTotal - volume_traded;
                        KF_LOG_DEBUG(logger,"VolumeTotal:"<<rtn_order.VolumeTotal<<" VolumeOriginal:"<<rtn_order.VolumeTotalOriginal);
                        if(rtn_order.VolumeTotal == 0) rtn_order.OrderStatus = LF_CHAR_AllTraded;
                        else rtn_order.OrderStatus = LF_CHAR_PartTradedQueueing;
                        strcpy(rtn_order.GatewayTime,std::to_string(cur_time).c_str());
                        rtn_orders.push_back(rtn_order);

                        memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
                        strcpy(rtn_trade.BrokerID,rtn_order.BrokerID);
                        strcpy(rtn_trade.UserID,rtn_order.UserID);
                        strcpy(rtn_trade.InvestorID,rtn_order.InvestorID);
                        strcpy(rtn_trade.BusinessUnit,rtn_order.BusinessUnit);
                        strcpy(rtn_trade.InstrumentID, rtn_order.InstrumentID);
                        strcpy(rtn_trade.OrderRef,rtn_order.OrderRef);
                        strcpy(rtn_trade.ExchangeID,exchange_name.c_str());
                        strcpy(rtn_trade.TradeID,getTimestampString().c_str());
                        strcpy(rtn_trade.OrderSysID,rtn_order.OrderRef);
                        rtn_trade.Direction = rtn_order.Direction;
                        rtn_trade.OffsetFlag = rtn_order.OffsetFlag;
                        rtn_trade.HedgeFlag = rtn_order.HedgeFlag;
                        rtn_trade.Price = price_traded;
                        rtn_trade.Volume = volume_traded;
                        strcpy(rtn_trade.TradeTime,getTimestampString().c_str());
                        rtn_trades.push_back(rtn_trade);

                        if(rtn_order.VolumeTotal == 0) // 该订单全部消耗光
                        {
                            iter = orderref_price_map.find(rtn_order.OrderRef);
                            if(iter != orderref_price_map.end()) orderref_price_map.erase(iter);                                    
                            iter2 = iter1->second.erase(iter2);
                        }
                        else
                        {
                            iter2->second.VolumeTraded = 0;
                            iter2->second.VolumeTotal = rtn_order.VolumeTotal;
                            // iter2->second.VolumeTotalOriginal = rtn_order.VolumeTotal;
                            iter2++;                                   
                        }                               
                    }                            
                }
                // 如果iter2全被消耗完，iter1也要删除
                if(iter1->second.size() < 1)
                {
                    iter1 = orderbook_it->second.orderbook_buy_line.erase(iter1);
                } 
                else
                {
                    iter1++;
                }                    
            }
        }
    }
    else
    {
        if(price >= sell_price)
        {
            for(iter1 = orderbook_it->second.orderbook_sell_line.begin(); iter1 != orderbook_it->second.orderbook_sell_line.end(); )
            {
                sell_price = iter1->first;
                if(price < sell_price) break;
                if(volume <= 0) break;
                for(iter2 = iter1->second.begin(); iter2 != iter1->second.end(); )
                {
                    if(volume <= 0) break;
                    int64_t sell_rcv_time = iter2->first;
                    rtn_order = iter2->second;
                    if(rtn_order.RequestID == -1) // order from 106
                    {

                        if(volume<= rtn_order.VolumeTotal)
                        {
                            volume=0; // 消耗完106的，但不消耗本地的
                        } 
                        else
                        {

                            volume -= rtn_order.VolumeTotal; 
                        }         
                        iter2++;                      
                    }
                    else // order from local
                    {

                        volume_traded = std::min(volume,rtn_order.VolumeTotal);
                        price_traded = std::min(price,rtn_order.LimitPrice);
                        // 买家回报 + 修改md
                        volume -= volume_traded;
                        // 卖家回报 + 修改orderbook_sell_line
                        rtn_order.VolumeTraded = volume_traded;
                        // rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - volume_traded;
                        rtn_order.VolumeTotal = rtn_order.VolumeTotal - volume_traded;
                        KF_LOG_DEBUG(logger,"VolumeTotal:"<<rtn_order.VolumeTotal<<" VolumeOriginal:"<<rtn_order.VolumeTotalOriginal);
                        if(rtn_order.VolumeTotal == 0) rtn_order.OrderStatus = LF_CHAR_AllTraded;
                        else rtn_order.OrderStatus = LF_CHAR_PartTradedQueueing;
                        strcpy(rtn_order.GatewayTime,std::to_string(cur_time).c_str());
                        rtn_orders.push_back(rtn_order);

                        memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
                        strcpy(rtn_trade.BrokerID,rtn_order.BrokerID);
                        strcpy(rtn_trade.UserID,rtn_order.UserID);
                        strcpy(rtn_trade.InvestorID,rtn_order.InvestorID);
                        strcpy(rtn_trade.BusinessUnit,rtn_order.BusinessUnit);
                        strcpy(rtn_trade.InstrumentID, rtn_order.InstrumentID);
                        strcpy(rtn_trade.OrderRef,rtn_order.OrderRef);
                        strcpy(rtn_trade.ExchangeID,exchange_name.c_str());
                        strcpy(rtn_trade.TradeID,getTimestampString().c_str());
                        strcpy(rtn_trade.OrderSysID,rtn_order.OrderRef);
                        rtn_trade.Direction = rtn_order.Direction;
                        rtn_trade.OffsetFlag = rtn_order.OffsetFlag;
                        rtn_trade.HedgeFlag = rtn_order.HedgeFlag;
                        rtn_trade.Price = price_traded;
                        rtn_trade.Volume = volume_traded;
                        strcpy(rtn_trade.TradeTime,getTimestampString().c_str());
                        rtn_trades.push_back(rtn_trade);

                        if(rtn_order.VolumeTotal == 0) // 该订单全部消耗光
                        {
                            iter = orderref_price_map.find(rtn_order.OrderRef);
                            if(iter != orderref_price_map.end()) orderref_price_map.erase(iter);                                    
                            iter2 = iter1->second.erase(iter2);
                        }
                        else
                        {
                            iter2->second.VolumeTraded = 0;
                            iter2->second.VolumeTotal = rtn_order.VolumeTotal;
                            // iter2->second.VolumeTotalOriginal = rtn_order.VolumeTotal;
                            iter2++;                                   
                        }                               
                    }                              
                }
                // 如果iter2全被消耗完，iter1也要删除
                if(iter1->second.size() < 1)
                {
                    iter1 = orderbook_it->second.orderbook_sell_line.erase(iter1);
                } 
                else
                {
                    iter1++;
                }  
            }
        }
    }
    this_thread::sleep_for(chrono::nanoseconds(trade_latency_ns));
    for(int i=0; i<rtn_orders.size(); i++ )
    {
        on_rtn_order(&rtn_orders[i]);
    }
    for(int i=0; i<rtn_trades.size(); i++)
    {
        on_rtn_trade(&rtn_trades[i]);
    }

}

void TDEngineMockBase::listening2()
{
    auto mapSources = get_map_sources();
    auto it = mapSources.find(source_id_105); // source_id_105
    if(it != mapSources.end())
    {
        JournalPair jp = getMdJournalPair(it->first);
        std::vector<std::string> folders,names;
        folders.push_back(jp.first);
        names.push_back(jp.second);
        reader2 = kungfu::yijinjing::JournalReader::createReaderWithSys(folders, names, kungfu::yijinjing::getNanoTime(), module_name);
    }
    yijinjing::FramePtr frame;
    AccountUnitMock& unit = account_units[0];
    while (isRunning && signal_received < 0)
    {
        frame = reader2->getNextFrame();
        if (frame.get() != nullptr)
        {
            short msg_type = frame->getMsgType();
            short msg_source = frame->getSource();
            cur_time = frame->getNano();
            if (msg_type == MSG_TYPE_LF_L2_TRADE) //105  如果105处理的订单是106发来的，则不处理
            {
                KF_LOG_INFO(logger, "[TDEngineMockBase::listening105] (msg_type)" << msg_type <<" (msg_source)"<< msg_source << " (cur_time)" << cur_time);
                void* fdata = frame->getData();
                LFL2TradeField md = *(LFL2TradeField*)fdata;
                
                std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(md.InstrumentID));
                if(ticker.length() == 0) {
                    KF_LOG_INFO(logger, std::string(md.InstrumentID) << " not in WhiteList, ignore it");
                    continue;
                }

                orderbook_lock.lock();//deribit bitmex 
                auto it2 = orderbook_map.find(md.InstrumentID);
                if(it2 != orderbook_map.end()){

                    KF_LOG_INFO(logger, "[TDEngineMockBase::listening105] (105_volume)" << md.Volume << " (105_price)" << md.Price);
                   // 从buy中读一条数据  Price <= buy_price 成交 
                   // 从sell中读一条数据 Price >= sell_price 成交
                   
                    int64_t buy_price,sell_price;
                    if(it2->second.orderbook_buy_line.empty()) buy_price = -1;
                    else buy_price = it2->second.orderbook_buy_line.begin()->first;
                    if(it2->second.orderbook_sell_line.empty()) sell_price = -1;
                    else sell_price = it2->second.orderbook_sell_line.begin()->first;
                    
                    vector<LFRtnOrderField> rtn_orders;
                    vector<LFRtnTradeField> rtn_trades;
                    LFRtnOrderField rtn_order;
                    LFRtnTradeField rtn_trade;
                    map<int64_t,map<int64_t,LFRtnOrderField> >::iterator iter1;
                    map<int64_t,LFRtnOrderField>::iterator iter2;
                    map<string,int64_t>::iterator iter;
                    uint64_t volume_traded;
                    int64_t price_traded;
                    
                    if( md.Price <= buy_price )
                    {
                        for(iter1 = it2->second.orderbook_buy_line.begin(); iter1 != it2->second.orderbook_buy_line.end(); )
                        {
                            buy_price = iter1->first;
                            if(md.Price > buy_price) break;
                            if(md.Volume <= 0) break;
                            for(iter2 = iter1->second.begin(); iter2 != iter1->second.end(); )
                            {
                                if(md.Volume <= 0) break;
                                int64_t buy_rcv_time = iter2->first;
                                rtn_order = iter2->second;
                                if(rtn_order.RequestID == -1) // order from 106 
                                {
                                    // if(md.Volume <= rtn_order.VolumeTotalOriginal)
                                    if(md.Volume <= rtn_order.VolumeTotal)
                                    {
                                        md.Volume =0; // 消耗完106的，但不消耗本地的
                                    } 
                                    else
                                    {
                                        // md.Volume -= rtn_order.VolumeTotalOriginal; 
                                        md.Volume -= rtn_order.VolumeTotal; 
                                    }   
                                    iter2++;                            
                                }
                                else // order from local
                                {
                                    
                                    // volume_traded = std::min(md.Volume,rtn_order.VolumeTotalOriginal);
                                    volume_traded = std::min(md.Volume,rtn_order.VolumeTotal);
                                    price_traded = rtn_order.LimitPrice;
                                    // 卖家回报 + 修改md
                                    md.Volume -= volume_traded;
                                    // 买家回报 + 修改orderbook_buy_line
                                    rtn_order.VolumeTraded = volume_traded;
                                    // rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - volume_traded;
                                    rtn_order.VolumeTotal = rtn_order.VolumeTotal - volume_traded;
                                    KF_LOG_DEBUG(logger,"VolumeTotal:"<<rtn_order.VolumeTotal<<" VolumeOriginal:"<<rtn_order.VolumeTotalOriginal);
                                    if(rtn_order.VolumeTotal == 0) rtn_order.OrderStatus = LF_CHAR_AllTraded;
                                    else rtn_order.OrderStatus = LF_CHAR_PartTradedQueueing;
                                    strcpy(rtn_order.GatewayTime,std::to_string(cur_time).c_str());
                                    rtn_orders.push_back(rtn_order);

                                    memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
                                    strcpy(rtn_trade.BrokerID,rtn_order.BrokerID);
                                    strcpy(rtn_trade.UserID,rtn_order.UserID);
                                    strcpy(rtn_trade.InvestorID,rtn_order.InvestorID);
                                    strcpy(rtn_trade.BusinessUnit,rtn_order.BusinessUnit);
                                    strcpy(rtn_trade.InstrumentID, rtn_order.InstrumentID);
                                    strcpy(rtn_trade.OrderRef,rtn_order.OrderRef);
                                    strcpy(rtn_trade.ExchangeID,exchange_name.c_str());
                                    strcpy(rtn_trade.TradeID,getTimestampString().c_str());
                                    strcpy(rtn_trade.OrderSysID,rtn_order.OrderRef);
                                    rtn_trade.Direction = rtn_order.Direction;
                                    rtn_trade.OffsetFlag = rtn_order.OffsetFlag;
                                    rtn_trade.HedgeFlag = rtn_order.HedgeFlag;
                                    rtn_trade.Price = price_traded;
                                    rtn_trade.Volume = volume_traded;
                                    strcpy(rtn_trade.TradeTime,getTimestampString().c_str());
                                    rtn_trades.push_back(rtn_trade);

                                    if(rtn_order.VolumeTotal == 0) // 该订单全部消耗光
                                    {
                                        iter = orderref_price_map.find(rtn_order.OrderRef);
                                        if(iter != orderref_price_map.end()) orderref_price_map.erase(iter);                                    
                                        iter2 = iter1->second.erase(iter2);
                                    }
                                    else
                                    {
                                        iter2->second.VolumeTraded = 0;
                                        iter2->second.VolumeTotal = rtn_order.VolumeTotal;
                                        // iter2->second.VolumeTotalOriginal = rtn_order.VolumeTotal;
                                        iter2++;                                   
                                    }                               
                                }                            
                            }
                            // 如果iter2全被消耗完，iter1也要删除
                            if(iter1->second.size() < 1)
                            {
                                iter1 = it2->second.orderbook_buy_line.erase(iter1);
                            } 
                            else
                            {
                                iter1++;
                            }                    
                        }
                    }
                    else if( sell_price != -1 && md.Price >= sell_price )
                    {
                        for(iter1 = it2->second.orderbook_sell_line.begin(); iter1 != it2->second.orderbook_sell_line.end(); )
                        {
                            sell_price = iter1->first;
                            if(md.Price < sell_price) break;
                            if(md.Volume <= 0) break;
                            for(iter2 = iter1->second.begin(); iter2 != iter1->second.end(); )
                            {
                                if(md.Volume <= 0) break;
                                int64_t sell_rcv_time = iter2->first;
                                rtn_order = iter2->second;
                                if(rtn_order.RequestID == -1) // order from 106
                                {
                                    // if(md.Volume <= rtn_order.VolumeTotalOriginal)
                                    if(md.Volume <= rtn_order.VolumeTotal)
                                    {
                                        md.Volume =0; // 消耗完106的，但不消耗本地的
                                    } 
                                    else
                                    {
                                        // md.Volume -= rtn_order.VolumeTotalOriginal; 
                                        md.Volume -= rtn_order.VolumeTotal; 
                                    }         
                                    iter2++;                      
                                }
                                else // order from local
                                {
                                    // volume_traded = std::min(md.Volume,rtn_order.VolumeTotalOriginal);
                                    volume_traded = std::min(md.Volume,rtn_order.VolumeTotal);
                                    price_traded = std::min(md.Price,rtn_order.LimitPrice);
                                    // 买家回报 + 修改md
                                    md.Volume -= volume_traded;
                                    // 卖家回报 + 修改orderbook_sell_line
                                    rtn_order.VolumeTraded = volume_traded;
                                    // rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - volume_traded;
                                    rtn_order.VolumeTotal = rtn_order.VolumeTotal - volume_traded;
                                    KF_LOG_DEBUG(logger,"VolumeTotal:"<<rtn_order.VolumeTotal<<" VolumeOriginal:"<<rtn_order.VolumeTotalOriginal);
                                    if(rtn_order.VolumeTotal == 0) rtn_order.OrderStatus = LF_CHAR_AllTraded;
                                    else rtn_order.OrderStatus = LF_CHAR_PartTradedQueueing;
                                    strcpy(rtn_order.GatewayTime,std::to_string(cur_time).c_str());
                                    rtn_orders.push_back(rtn_order);

                                    memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
                                    strcpy(rtn_trade.BrokerID,rtn_order.BrokerID);
                                    strcpy(rtn_trade.UserID,rtn_order.UserID);
                                    strcpy(rtn_trade.InvestorID,rtn_order.InvestorID);
                                    strcpy(rtn_trade.BusinessUnit,rtn_order.BusinessUnit);
                                    strcpy(rtn_trade.InstrumentID, rtn_order.InstrumentID);
                                    strcpy(rtn_trade.OrderRef,rtn_order.OrderRef);
                                    strcpy(rtn_trade.ExchangeID,exchange_name.c_str());
                                    strcpy(rtn_trade.TradeID,getTimestampString().c_str());
                                    strcpy(rtn_trade.OrderSysID,rtn_order.OrderRef);
                                    rtn_trade.Direction = rtn_order.Direction;
                                    rtn_trade.OffsetFlag = rtn_order.OffsetFlag;
                                    rtn_trade.HedgeFlag = rtn_order.HedgeFlag;
                                    rtn_trade.Price = price_traded;
                                    rtn_trade.Volume = volume_traded;
                                    strcpy(rtn_trade.TradeTime,getTimestampString().c_str());
                                    rtn_trades.push_back(rtn_trade);

                                    if(rtn_order.VolumeTotal == 0) // 该订单全部消耗光
                                    {
                                        iter = orderref_price_map.find(rtn_order.OrderRef);
                                        if(iter != orderref_price_map.end()) orderref_price_map.erase(iter);                                    
                                        iter2 = iter1->second.erase(iter2);
                                    }
                                    else
                                    {
                                        iter2->second.VolumeTraded = 0;
                                        iter2->second.VolumeTotal = rtn_order.VolumeTotal;
                                        // iter2->second.VolumeTotalOriginal = rtn_order.VolumeTotal;
                                        iter2++;                                   
                                    }                               
                                }                              
                            }
                            // 如果iter2全被消耗完，iter1也要删除
                            if(iter1->second.size() < 1)
                            {
                                iter1 = it2->second.orderbook_sell_line.erase(iter1);
                            } 
                            else
                            {
                                iter1++;
                            }  
                        }
                    }
                    
                   // send rtn
                    this_thread::sleep_for(chrono::nanoseconds(trade_latency_ns));
                    for(int i=0; i<rtn_orders.size(); i++ )
                    {
                        on_rtn_order(&rtn_orders[i]);
                    }
                    for(int i=0; i<rtn_trades.size(); i++)
                    {
                        on_rtn_trade(&rtn_trades[i]);
                    }
                    write_orderbook(md.InstrumentID,md.ExchangeID,cur_time,it2);

                }
                orderbook_lock.unlock();
            }
            else if(msg_type == MSG_TYPE_LF_PRICE_BOOK_20) // 106数据，更新orderbook，双向遍历
            {
                KF_LOG_INFO(logger, "[TDEngineMockBase::listening2] (msg_type)" << msg_type <<" (msg_source)"<< msg_source << " (cur_time)" << cur_time);
                void* fdata = frame->getData();
                LFPriceBook20Field md = *(LFPriceBook20Field*)fdata;
                
                std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(md.InstrumentID));
                if(ticker.length() == 0) {
                    KF_LOG_INFO(logger, std::string(md.InstrumentID) << " not in WhiteList, ignore it");
                    continue;
                }

                orderbook_lock.lock();//
                auto it2 = orderbook_map.find(md.InstrumentID);
                if(it2 != orderbook_map.end()){

                    KF_LOG_INFO(logger, "[TDEngineMockBase::listening2] (BidLevelCount)"<<md.BidLevelCount<<"(AskLevelCount)"<<md.AskLevelCount<<"(bid_price)"<<md.BidLevels[0].price<<"(ask_price)"<<md.AskLevels[0].price);
                    map<int64_t,map<int64_t,LFRtnOrderField> >::iterator iter1;
                    map<int64_t,LFRtnOrderField>::iterator iter2;
                    map<int64_t,LFRtnOrderField>::reverse_iterator riter2;
                    LFRtnOrderField rtn_order;
                    int index = 0;
                    memset(&rtn_order, 0, sizeof(LFRtnOrderField));
                    strcpy(rtn_order.InstrumentID,md.InstrumentID);
                    strcpy(rtn_order.ExchangeID,md.ExchangeID);
                    
                    for(int i = 0 ; i < md.BidLevelCount ; i++) // 处理orderbook_buy_line中有106的level部分
                    {
                        int64_t bid_price = md.BidLevels[i].price;
                        uint64_t bid_volume = md.BidLevels[i].volume;
                        iter1 = it2->second.orderbook_buy_line.find(bid_price);
                        if(iter1 != it2->second.orderbook_buy_line.end())
                        {
                            // 统计orderbook中来自106的总数量
                            uint64_t bid_sum = 0;
                            for(iter2 = iter1->second.begin(); iter2 != iter1->second.end() ; iter2++)
                            {
                                if(iter2->second.RequestID == -1)
                                {
                                    // bid_sum += iter2->second.VolumeTotalOriginal;
                                    bid_sum += iter2->second.VolumeTotal;
                                }
                            }
                            if(bid_sum < bid_volume) 
                            {
                                // 增加订单数 bid_volume - bid_sum
                                riter2 = iter1->second.rbegin();
                                if(riter2->second.RequestID == -1)
                                {
                                    // 直接修改最后一条
                                    // riter2->second.VolumeTotalOriginal += (bid_volume - bid_sum);
                                    riter2->second.VolumeTotalOriginal += (bid_volume - bid_sum);
                                    riter2->second.VolumeTotal += (bid_volume - bid_sum);
                                }
                                else
                                {
                                    // 新增一个
                                    rtn_order.LimitPrice = bid_price;
                                    KF_LOG_DEBUG(logger,"listening  LimitPrice is %ld"<< bid_price);
                                    rtn_order.RequestID = -1;
                                    // rtn_order.VolumeTotalOriginal = bid_volume - bid_sum;
                                    rtn_order.VolumeTotalOriginal = bid_volume - bid_sum;
                                    rtn_order.VolumeTotal = bid_volume - bid_sum;
                                    rtn_order.VolumeTraded = rtn_order.VolumeTotal = 0;
                                    iter1->second.insert(make_pair(cur_time,rtn_order));
                                }
                            }
                            else if(bid_sum > bid_volume)
                            {
                                // 按顺序减少订单数
                                uint64_t del_volume = bid_sum - bid_volume;
                                for(iter2 = iter1->second.begin(); iter2 != iter1->second.end() ; )
                                {
                                    if(del_volume == 0) break;
                                    if(iter2->second.RequestID == -1)
                                    {
                                        // if(iter2->second.VolumeTotalOriginal <= del_volume)
                                        if(iter2->second.VolumeTotal <= del_volume)
                                        {
                                            // 将该订单删除
                                            iter2 = iter1->second.erase(iter2);
                                            // del_volume -= iter2->second.VolumeTotalOriginal;
                                            del_volume -= iter2->second.VolumeTotal;
                                            continue;
                                        } 
                                        else
                                        {
                                            // 该订单的volume减少
                                            // iter2->second.VolumeTotalOriginal -= del_volume;
                                            iter2->second.VolumeTotalOriginal -= del_volume;
                                            iter2->second.VolumeTotal -= del_volume;
                                            del_volume = 0; 
                                            break;
                                        }  
                                    }
                                    iter2 ++;
                                }
                                if(iter1->second.size() < 1) it2->second.orderbook_buy_line.erase(iter1);
                            }
                        }
                        else
                        {

                            updateOrderBook106(LF_CHAR_Buy,bid_price,bid_volume,it2);
                            // new a level
                            if(bid_volume<=0)
                            continue;
                            rtn_order.LimitPrice = bid_price;
                            KF_LOG_DEBUG(logger,"listening 2 LimitPrice is %ld"<< bid_price);
                            rtn_order.RequestID = -1;
                            // rtn_order.VolumeTotalOriginal = bid_volume;
                            rtn_order.VolumeTotalOriginal = bid_volume;
                            rtn_order.VolumeTotal = bid_volume;
                            rtn_order.Direction = LF_CHAR_Buy;
                            rtn_order.VolumeTraded = 0;
                            it2->second.orderbook_buy_line.insert(make_pair(bid_price,map<int64_t,LFRtnOrderField>()));
                            it2->second.orderbook_buy_line[bid_price].insert(make_pair(cur_time,rtn_order));
                        }                   
                    }
                    // orderbook_buy_line BidLevel中价格从高到低
                    index = 0;
                    for(iter1 = it2->second.orderbook_buy_line.begin() ; iter1 != it2->second.orderbook_buy_line.end() ; ) // orderbook_buy_line中没有106的level的部分
                    {
                        while(index < md.BidLevelCount && iter1->first < md.BidLevels[index].price) index++;
                        if(index < md.BidLevelCount && iter1->first == md.BidLevels[index].price) 
                        {
                            iter1++;
                            index++;
                            continue;
                        }
                        for(iter2 = iter1->second.begin() ; iter2 != iter1->second.end() ;)
                        {
                            if(iter2->second.RequestID == -1) iter2 = iter1->second.erase(iter2);
                            else iter2++;
                        }
                        if(iter1->second.size() < 1) iter1 = it2->second.orderbook_buy_line.erase(iter1);
                        else iter1++;
                    }

                    for(int i = 0 ; i < md.AskLevelCount ; i++) // orderbook_sell_line
                    {
                        int64_t ask_price = md.AskLevels[i].price;
                        uint64_t ask_volume = md.AskLevels[i].volume;
                        iter1 = it2->second.orderbook_sell_line.find(ask_price);
                        if(iter1 != it2->second.orderbook_sell_line.end())
                        {
                            // 统计orderbook中来自106的总数量
                            uint64_t ask_sum = 0; 
                            for(iter2 = iter1->second.begin(); iter2 != iter1->second.end() ; iter2++)
                            {
                                if(iter2->second.RequestID == -1)
                                {
                                    // ask_sum += iter2->second.VolumeTotalOriginal;
                                    ask_sum += iter2->second.VolumeTotal;
                                }
                            }
                            if(ask_sum < ask_volume)
                            {
                                // 增加订单数 ask_volume - ask_sum
                                riter2 = iter1->second.rbegin();
                                if(riter2->second.RequestID == -1)
                                {
                                    // 直接修改最后一条
                                    // riter2->second.VolumeTotalOriginal += (ask_volume - ask_sum);
                                    riter2->second.VolumeTotalOriginal += (ask_volume - ask_sum);
                                    riter2->second.VolumeTotal += (ask_volume - ask_sum);
                                }
                                else
                                {
                                    // 新增一个
                                    rtn_order.LimitPrice = ask_price;
                                    KF_LOG_DEBUG(logger,"listening2 2 LimitPrice is %ld"<< ask_price);
                                    rtn_order.RequestID = -1;
                                    // rtn_order.VolumeTotalOriginal = ask_volume - ask_sum;
                                    rtn_order.VolumeTotalOriginal = ask_volume - ask_sum;
                                    rtn_order.VolumeTotal = ask_volume - ask_sum;
                                    rtn_order.VolumeTraded = 0;
                                    iter1->second.insert(make_pair(cur_time,rtn_order));
                                }
                            }
                            else if(ask_sum > ask_volume)
                            {
                                // 按顺序减少订单数
                                uint64_t del_volume = ask_sum - ask_volume;
                                for(iter2 = iter1->second.begin(); iter2 != iter1->second.end() ; )
                                {
                                    if(del_volume == 0) break;
                                    if(iter2->second.RequestID == -1)
                                    {
                                        // if(iter2->second.VolumeTotalOriginal <= del_volume)
                                        if(iter2->second.VolumeTotal <= del_volume)
                                        {
                                            // 将该订单删除
                                            iter2 = iter1->second.erase(iter2);
                                            // del_volume -= iter2->second.VolumeTotalOriginal;
                                            del_volume -= iter2->second.VolumeTotal;
                                            continue;
                                        } 
                                        else
                                        {
                                            // 该订单的volume减少
                                            // iter2->second.VolumeTotalOriginal -= del_volume;
                                            iter2->second.VolumeTotal -= del_volume;
                                            del_volume = 0; 
                                            break;
                                        }  
                                    }
                                    iter2 ++;
                                }
                                if(iter1->second.size() < 1) it2->second.orderbook_sell_line.erase(iter1);
                            }
                        }
                        else
                        {
                            // new a level
                            updateOrderBook106(LF_CHAR_Sell,ask_price,ask_volume,it2);
                            if(ask_volume<=0)
                            continue;
                            rtn_order.LimitPrice = ask_price;
                            KF_LOG_DEBUG(logger,"listening2 3 LimitPrice is %ld"<< ask_price);
                            rtn_order.RequestID = -1;
                            rtn_order.VolumeTotalOriginal = ask_volume;
                            rtn_order.VolumeTotal = ask_volume;
                            rtn_order.VolumeTraded =  0;
                            rtn_order.Direction = LF_CHAR_Sell;
                            it2->second.orderbook_sell_line.insert(make_pair(ask_price,map<int64_t,LFRtnOrderField>()));
                            it2->second.orderbook_sell_line[ask_price].insert(make_pair(cur_time,rtn_order));
                        }
                    }
                    // orderbook_sell_line AskLevel中价格从低到高
                    index = 0;
                    for(iter1 = it2->second.orderbook_sell_line.begin() ; iter1 != it2->second.orderbook_sell_line.end() ; ) // orderbook_sell_line中没有106的level的部分
                    {
                        while(index < md.AskLevelCount && iter1->first > md.AskLevels[index].price) index++;
                        if(index < md.AskLevelCount && iter1->first == md.AskLevels[index].price) 
                        {
                            iter1++;
                            index++;
                            continue;
                        }
                        for(iter2 = iter1->second.begin() ; iter2 != iter1->second.end() ;)
                        {
                            if(iter2->second.RequestID == -1) iter2 = iter1->second.erase(iter2);
                            else iter2++;
                        }
                        if(iter1->second.size() < 1) iter1 = it2->second.orderbook_sell_line.erase(iter1);
                        else iter1++;
                    }

                    
                    write_orderbook(md.InstrumentID,md.ExchangeID,cur_time,it2);

                }
                orderbook_lock.unlock();
            }
        }
    }

    if (IEngine::signal_received >= 0)
    {
        KF_LOG_INFO(logger, "[IEngine] signal received: " << IEngine::signal_received);
    }

    if (!isRunning)
    {
        KF_LOG_INFO(logger, "[IEngine] forced to stop.");
    }    
}

void TDEngineMockBase::load_history_data()
{

    std::ifstream inFile(history_data_path, std::ios::in);
    std::string strLine;
    if (inFile.is_open())
    {
        KF_LOG_INFO(logger, module_name << " start read history data!");
        while (getline(inFile, strLine))
        {
            std::stringstream ss(strLine);
            std::string sub_str;
            std::vector<std::string> tmp;

            while (getline(ss, sub_str, ','))
            {
                tmp.emplace_back(sub_str);
            }
            mockData.emplace_back(tmp);
        }
        load_data_flag = true;
        KF_LOG_INFO(logger, "TDEngineMockBase::req_get_kline_via_rest(): mockData.size():" << mockData.size());
    }
    else
    {
        // Log
        KF_LOG_INFO(logger, "Mock-TDEngineMockBase::req_get_kline_via_rest: open file error!");
    }
    return;
}

void TDEngineMockBase::req_get_kline_via_rest(const GetKlineViaRest *data, int account_index, int requestId, long rcv_time)
{
    KF_LOG_INFO(logger, "TDEngineMockBase::req_get_kline_via_rest: (symbol)" << data->Symbol << " (interval)" << data->Interval << " (IgnoreStartTime)" << data->IgnoreStartTime);

    writer->write_frame(data, sizeof(GetKlineViaRest), source_id, MSG_TYPE_LF_GET_KLINE_VIA_REST, 1 /*islast*/, requestId);

    AccountUnitMock &unit = account_units[account_index];
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(data->Symbol);
    if (ticker.empty())
    {
        KF_LOG_INFO(logger, "symbol not in white list");
        return;
    }

    int param_limit = data->Limit;
    if (param_limit > LIMIT)
        param_limit = LIMIT;
    else if (param_limit < 1)
        param_limit = 1;

    if (!load_data_flag)
        load_history_data();

    if (load_data_flag)
    {
        LFBarSerial1000Field bars;
        memset(&bars, 0, sizeof(bars));
        strncpy(bars.InstrumentID, data->Symbol, 31);
        strcpy(bars.ExchangeID, "binance");

        int left = 0, right = mockData.size() - 1;
        while (left < right)
        {
            int mid = left + right + 1 >> 1;
            if (string(data->MiscInfo).length() > 23)
            {
                if (mockData[left][6] <= string(data->MiscInfo).substr(4, 23))
                {
                    left = mid;
                }
                else
                {
                    right = mid - 1;
                }
            } else {

            }
        }
        int count = 0;
        for (int i = left; i < int(mockData.size()) && count < LIMIT; i++)
        {
            // KF_LOG_INFO(logger, mockData[i][0] << " " << mockData[i][1] << "  " << mockData[i][2] << " " << mockData[i][3] << " " << mockData[i][4] << " " << mockData[i][5] << " " << mockData[i][6] << " " << mockData[i][7] << " " << mockData[i][8]);
            if (history_date_type == -1)
            {
                KF_LOG_DEBUG(logger, "history data type configure error !");
                break;
            }
            else if (history_date_type == 110)
            {
		        //KF_LOG_INFO(logger, "zyk" << mockData[i][0] << " " << data->Symbol);
                if (mockData[i][1] != data->Symbol)
                    continue;
                if (mockData[i].size() < 14)
                {
                    KF_LOG_ERROR(logger, "mockData.size()" << mockData.size());
                    break;
                }
                
                KF_LOG_INFO(logger, mockData[i][6] << " " << mockData[i][8] << "  " << mockData[i][10] << " " << mockData[i][11] << " " << mockData[i][12] << " " << mockData[i][13] << " " << mockData[i][14]);
                // 这里直接查看对应csv的列
                int64_t nStartTime = std::stoll(mockData[i][6]);
                int64_t nEndTime = std::stoll(mockData[i][8].substr(0, 13));
                bars.BarSerial[count].StartUpdateMillisec = nStartTime;
                bars.BarSerial[count].EndUpdateMillisec = nEndTime;
                bars.BarSerial[count].PeriodMillisec = nEndTime - nStartTime + 1;

                bars.BarSerial[count].Open = std::stoll(mockData[i][10]);
                bars.BarSerial[count].Close = std::stoll(mockData[i][11]);
                bars.BarSerial[count].Low = std::stoll(mockData[i][12]);
                bars.BarSerial[count].High = std::stoll(mockData[i][13]);

                bars.BarSerial[count].Volume = std::stoull(mockData[i][14]);
            }
            else if (history_date_type == 219110)
            {
                if (mockData[i][0] != data->Symbol)
                    continue;
                if (mockData[i].size() < 8)
                {
                    KF_LOG_ERROR(logger, "mockData.size()" << mockData.size());
                    break;
                }
                // 这里直接查看对应csv的列
                int64_t nStartTime = std::stoll(mockData[i][7]);
                int64_t nEndTime = std::stoll(mockData[i][8].substr(0, 13));
                bars.BarSerial[count].StartUpdateMillisec = nStartTime;
                bars.BarSerial[count].EndUpdateMillisec = nEndTime;
                bars.BarSerial[count].PeriodMillisec = nEndTime - nStartTime + 1;

                bars.BarSerial[count].Open = std::stoll(mockData[i][2]);
                bars.BarSerial[count].Close = std::stoll(mockData[i][3]);
                bars.BarSerial[count].Low = std::stoll(mockData[i][4]);
                bars.BarSerial[count].High = std::stoll(mockData[i][5]);

                bars.BarSerial[count].Volume = std::stoull(mockData[i][6]);
            }

            bars.BarSerial[count].TransactionsNum = 0;

            bars.BarSerial[count].PeriodMillisec = period_millisec;
            count++;
        }
        bars.BarLevel = count;
        on_bar_serial1000(&bars, data->RequestID);
    }
}

void TDEngineMockBase::write_orderbook(const char* InstrumentID, const char* ExchangeID, uint64_t cur_time, std::map<std::string, OrderBook>::iterator orderbook_it) // 将orderbook的状态用106到td journal
{               
    LFPriceBook20Field data;
    strcpy(data.InstrumentID,InstrumentID);
    strcpy(data.ExchangeID,ExchangeID);
    data.UpdateMicroSecond = cur_time;
    
    data.BidLevelCount = orderbook_it->second.orderbook_buy_line.size();
    auto iter1 = orderbook_it->second.orderbook_buy_line.begin();
    int cnt=0;
    for( ;iter1!=orderbook_it->second.orderbook_buy_line.end();iter1++)
    {
        // data.BidLevels[cnt]
        if(cnt>=20) break;
        data.BidLevels[cnt].price = iter1->first;
        uint64_t total_volume = 0;
        auto iter2 = iter1->second.begin();
        for( ;iter2!=iter1->second.end();iter2++)
        {
            // total_volume += iter2->second.VolumeTotalOriginal;
            total_volume += iter2->second.VolumeTotal;
        }
        data.BidLevels[cnt].volume = total_volume;
        cnt++;
    }
    data.AskLevelCount = orderbook_it->second.orderbook_sell_line.size();
    iter1 = orderbook_it->second.orderbook_sell_line.begin();
    cnt=0;
    for( ;iter1!=orderbook_it->second.orderbook_sell_line.end();iter1++)
    {
        // data.BidLevels[cnt]
        if(cnt>=20) break;
        data.AskLevels[cnt].price = iter1->first;
        uint64_t total_volume = 0;
        auto iter2 = iter1->second.begin();
        for( ;iter2!=iter1->second.end();iter2++)
        {
            // total_volume += iter2->second.VolumeTotalOriginal;
            total_volume += iter2->second.VolumeTotal;
        }
        data.AskLevels[cnt].volume = total_volume;
        cnt++;
    }      
    
    writer->write_frame(&data, sizeof(LFPriceBook20Field), source_id, source_id * 1000 + MSG_TYPE_LF_PRICE_BOOK_20, 1, -1);
    KF_LOG_DEBUG_FMT(logger, "price book 20 update: %-10s %d | %d [%ld, %lu | %ld, %lu] %d",
             data.InstrumentID,
             data.BidLevelCount,
             data.AskLevelCount,
             data.BidLevels[0].price,
             data.BidLevels[0].volume,
             data.AskLevels[0].price,
             data.AskLevels[0].volume,
             data.Status);
}

std::string TDEngineMockBase::getTimestampString()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  std::to_string(timestamp);
}

