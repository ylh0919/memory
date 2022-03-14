#include "TDEngineMockKuCoin.h"
#include "longfist/ctp.h"
#include "longfist/LFUtils.h"
#include <writer.h>
#include <stringbuffer.h>
#include <document.h>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <cpr/cpr.h>
#include <chrono>
#include <mutex>
#include <random>
using namespace std;
USING_WC_NAMESPACE

mutex m_mutexOrder;
TDEngineMockKuCoin::TDEngineMockKuCoin(): ITDEngine(SOURCE_MOCKKUCOIN)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.MockKuCoin");
    KF_LOG_INFO(logger, "[TDEngineMockKuCoin]");
}

TDEngineMockKuCoin::~TDEngineMockKuCoin()
{
}

void TDEngineMockKuCoin::init()
{
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalSafeWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
    KF_LOG_INFO(logger, "[init]");
}

void TDEngineMockKuCoin::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "[pre_load]");
}

void TDEngineMockKuCoin::resize_accounts(int account_num)
{
    account_units.resize(account_num);
    KF_LOG_INFO(logger, "[resize_accounts]");
}

TradeAccount TDEngineMockKuCoin::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "[load_account]");
    // internal load
    AccountUnitMockKuCoin& unit = account_units[idx];
    if(j_config.find("response_latency_ms") != j_config.end()) {
        unit.response_latency_ms = j_config["response_latency_ms"].get<int>();
    }
    if(j_config.find("match_latency_ms") != j_config.end()) {
        unit.match_latency_ms = j_config["match_latency_ms"].get<int>();
    }
    if(j_config.find("limit_exe_price_diff") != j_config.end()) {
        unit.limit_exe_price_diff = j_config["limit_exe_price_diff"].get<double>();
    }
    if(j_config.find("market_exe_price_diff") != j_config.end()) {
        unit.market_exe_price_diff = j_config["market_exe_price_diff"].get<double>();
    }
    if(j_config.find("partial_exe_count") != j_config.end()) {
        unit.partial_exe_count = j_config["partial_exe_count"].get<int>();
    }
    if(j_config.find("total_trade_count") != j_config.end()) {
        unit.total_trade_count = j_config["total_trade_count"].get<int>();
    }
    if(j_config.find("match_interval_ms") != j_config.end()) {
        unit.match_interval_ms = j_config["match_interval_ms"].get<int>();
    }
    
    unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    unit.coinPairWhiteList.Debug_print();

    unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
    unit.positionWhiteList.Debug_print();

    //display usage:
    if(unit.coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().size() == 0) {
        KF_LOG_ERROR(logger, "TDEngineMockKuCoin::load_account: subscribeCoinmexBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    //cancel all openning orders on TD startup

    // set up
    TradeAccount account = {};
    return account;
}


void TDEngineMockKuCoin::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[connect]");
    
}


void TDEngineMockKuCoin::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "[login]");
    connect(timeout_nsec);
}

void TDEngineMockKuCoin::logout()
{
    KF_LOG_INFO(logger, "[logout]");
}

void TDEngineMockKuCoin::release_api()
{
    KF_LOG_INFO(logger, "[release_api]");
}

bool TDEngineMockKuCoin::is_logged_in() const
{
    return true;
}

bool TDEngineMockKuCoin::is_connected() const
{
    KF_LOG_INFO(logger, "[is_connected]");
    return is_logged_in();
}

void TDEngineMockKuCoin::set_reader_thread()
{
    ITDEngine::set_reader_thread();
    rest_thread = ThreadPtr(new std::thread(boost::bind(&TDEngineMockKuCoin::loop, this)));
}

/**
 * req functions
 */
void TDEngineMockKuCoin::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_investor_position] (requestId)" << requestId);

    AccountUnitMockKuCoin& unit = account_units[account_index];
    KF_LOG_INFO(logger, "[req_investor_position] (InstrumentID) " << data->InstrumentID);

    int errorId = 0;
    std::string errorMsg = "";

    send_writer->write_frame(data, sizeof(LFQryPositionField), source_id, MSG_TYPE_LF_QRY_POS_MOCK, 1, requestId);

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

void TDEngineMockKuCoin::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    KF_LOG_INFO(logger, "[req_qry_account]");
}



void TDEngineMockKuCoin::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitMockKuCoin& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (Tid)" << data->InstrumentID
                                              << " (Volume)" << data->Volume
                                              << " (LimitPrice)" << data->LimitPrice
                                              << " (OrderRef)" << data->OrderRef);
    send_writer->write_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_MOCKKUCOIN, 1/*ISLAST*/, requestId);

    int errorId = 0;
    std::string errorMsg = "";
    on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList, ignore it  (rid)" << requestId <<
                                                                                      " (errorId)" << errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_COINMEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }
    KF_LOG_DEBUG(logger, "[req_order_insert] (exchange_ticker)" << ticker);

    if(errorId != 0)
    {
        on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
    }
    raw_writer->write_error_frame(data, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER_MOCK, 1, requestId, errorId, errorMsg.c_str());

    OrderStatus orderStatus;
    orderStatus.lastChangeTime = get_timestamp_ms();
    orderStatus.expect_price = data->ExpectPrice;
    LFRtnOrderField& rtn_order = orderStatus.order;
    memset(&rtn_order, 0, sizeof(LFRtnOrderField));
    rtn_order.OrderStatus = LF_CHAR_OrderInserted;
    rtn_order.VolumeTraded = 0;
    //first send onRtnOrder about the status change or VolumeTraded change
    strcpy(rtn_order.ExchangeID, "mockkucoin");
    strncpy(rtn_order.UserID, "mockkucoin", 16);
    strncpy(rtn_order.InstrumentID, data->InstrumentID, 31);
    rtn_order.Direction = data->Direction;
    //No this setting on coinmex
    rtn_order.TimeCondition = data->TimeCondition;
    rtn_order.OrderPriceType = data->OrderPriceType;
    strncpy(rtn_order.OrderRef, data->OrderRef, 13);
    rtn_order.VolumeTotalOriginal = data->Volume;
    rtn_order.LimitPrice = data->LimitPrice;
    rtn_order.VolumeTotal = rtn_order.VolumeTotalOriginal - rtn_order.VolumeTraded;

    //on_rtn_order(&rtn_order);
    //raw_writer->write_frame(&rtn_order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_MOCKKUCOIN,1, rtn_order.RequestID);
    //if(unit.total_trade_count == -1 || unit.total_trade_count > total_traded_orders)
    {
        std::lock_guard<std::mutex> lck(m_mutexOrder);
        mapOrders.insert(std::make_pair(data->OrderRef,orderStatus));   
    }

}


void TDEngineMockKuCoin::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    AccountUnitMockKuCoin& unit = account_units[account_index];
    KF_LOG_DEBUG(logger, "[req_order_action999]" << " (rid)" << requestId
                                              << " (Iid)" << data->InvestorID
                                              << " (OrderRef)" << data->OrderRef
                                              << " (KfOrderID)" << data->KfOrderID);

    send_writer->write_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_MOCK, 1, requestId);

    int errorId = 0;
    std::string errorMsg = "";

    std::string ticker = unit.coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if(ticker.length() == 0) {
        errorId = 200;
        errorMsg = std::string(data->InstrumentID) + " not in WhiteList, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
        raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_COINMEX, 1, requestId, errorId, errorMsg.c_str());
        return;
    }

    KF_LOG_DEBUG(logger, "[req_order_action] (exchange_ticker)" << ticker);
    std::lock_guard<std::mutex> lck(m_mutexOrder);
    auto it = mapOrders.find(data->OrderRef);
    if(it != mapOrders.end())
    {
        auto& order = it->second.order;
        order.OrderStatus = LF_CHAR_Canceled;
        on_rtn_order(&order);
        mapOrders.erase(it);
    }
    else
    {
        errorId = 201;
        errorMsg = std::string(data->OrderRef) + " is closed, ignore it";
        KF_LOG_ERROR(logger, "[req_order_action]: not in WhiteList , ignore it: (rid)" << requestId << " (errorId)" <<
                                                                                       errorId << " (errorMsg) " << errorMsg);
    }
    

    if(errorId != 0)
    {
        on_rsp_order_action(data, requestId, errorId, errorMsg.c_str());
    }
    raw_writer->write_error_frame(data, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_MOCK, 1, requestId, errorId, errorMsg.c_str());
}

int64_t TDEngineMockKuCoin::get_timestamp_ms()
{
    using namespace std::chrono;
    int64_t current_ms = duration_cast< milliseconds>(system_clock::now().time_since_epoch()).count();
    return current_ms;
}

void TDEngineMockKuCoin::loop()
{
    default_random_engine dre; 
    uniform_real_distribution<double> urd(0, 1); //随机数分布对象
    int64_t last_trade_time = 0;
    while(isRunning)
    {
        auto& unit = account_units[0];
        unique_lock<mutex> lck(m_mutexOrder);
        int64_t current_ms = get_timestamp_ms();
        auto it = mapOrders.begin();
        while(it != mapOrders.end())
        {
             auto& order = it->second.order;
            int64_t time_diff = current_ms - it->second.lastChangeTime;
            if(order.OrderStatus == LF_CHAR_OrderInserted && time_diff > unit.response_latency_ms)
            {//返回NotTouched 状态
                order.OrderStatus = LF_CHAR_NotTouched;
                on_rtn_order(&order);
                raw_writer->write_frame(&order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_MOCKBITMEX, 1, -1);
                it->second.lastChangeTime = get_timestamp_ms();
            }
            else if((unit.total_trade_count == -1 || total_traded_orders < unit.total_trade_count) && 
                (order.OrderStatus == LF_CHAR_NotTouched || order.OrderStatus == LF_CHAR_PartTradedQueueing) &&
                (time_diff > unit.match_latency_ms && current_ms - last_trade_time > unit.match_interval_ms))
            {//根据配置产生 部分成交 或 全部成交 回报
                int64_t volume = 0;
                if(it->second.exe_count < unit.partial_exe_count)
                {
                    double rate = urd(dre);
                    KF_LOG_DEBUG(logger, "[loop] trade, volume rate:"<<rate);
                    volume = std::round((order.VolumeTotalOriginal*1.0/scale_offset*rate)*scale_offset);
                    if(volume > order.VolumeTotal)
                    {
                        order.OrderStatus = LF_CHAR_AllTraded;
                        volume = order.VolumeTotal;
                        order.VolumeTotal = 0;
                        order.VolumeTraded = order.VolumeTotalOriginal;
                    }
                    else
                    {
                       order.VolumeTotal -= volume;
                       order.VolumeTraded+=volume;
                       order.OrderStatus = LF_CHAR_PartTradedQueueing;
                    }
                    it->second.exe_count++;
                }
                else
                {
                    order.OrderStatus = LF_CHAR_AllTraded;
                    volume = order.VolumeTotal;
                    order.VolumeTotal = 0;
                    order.VolumeTraded = order.VolumeTotalOriginal; 
                }
                on_rtn_order(&order);
                raw_writer->write_frame(&order, sizeof(LFRtnOrderField),source_id, MSG_TYPE_LF_RTN_ORDER_MOCKBITMEX, 1, -1);
                int64_t price = 0;
                double price_diff = 0;
                if(order.OrderPriceType == LF_CHAR_AnyPrice)
                {
                    price = it->second.expect_price;
                    price_diff = std::round(it->second.expect_price * unit.market_exe_price_diff);
                }
                else
                {
                    price = order.LimitPrice;
                    price_diff = std::round(order.LimitPrice * unit.limit_exe_price_diff);
                }
                if(order.Direction == LF_CHAR_Buy )
                {
                    price -= price_diff;
                }
                else
                {
                    price += price_diff;
                }
                LFRtnTradeField rtn_trade;
                memset(&rtn_trade, 0, sizeof(LFRtnTradeField));
                strcpy(rtn_trade.ExchangeID, "mockbitmex");
                strncpy(rtn_trade.UserID,order.UserID, sizeof(rtn_trade.UserID));
                strncpy(rtn_trade.InstrumentID, order.InstrumentID, sizeof(rtn_trade.InstrumentID));
                strncpy(rtn_trade.OrderRef, order.OrderRef, sizeof(rtn_trade.OrderRef));
                rtn_trade.Direction = order.Direction;
                //calculate the volumn and price (it is average too)
                rtn_trade.Volume = volume;
                rtn_trade.Price = price;
                strncpy(rtn_trade.OrderSysID,order.OrderRef,sizeof(rtn_trade.OrderSysID));
                sprintf(rtn_trade.TradeTime,"%ld",get_timestamp_ms());
                strncpy(rtn_trade.ClientID,order.OrderRef, sizeof(rtn_trade.ClientID));
                on_rtn_trade(&rtn_trade);
                raw_writer->write_frame(&rtn_trade, sizeof(LFRtnTradeField),source_id, MSG_TYPE_LF_RTN_TRADE_MOCKBITMEX, 1, -1);
                it->second.lastChangeTime = get_timestamp_ms();
                last_trade_time = get_timestamp_ms();
            }
            if(order.OrderStatus == LF_CHAR_AllTraded)
            {
                ++total_traded_orders;
                it = mapOrders.erase(it);
            }
            else
            {
                ++it;
            }         
        }
        lck.unlock();
        this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

BOOST_PYTHON_MODULE(libmockkucointd)
{
    using namespace boost::python;
    class_<TDEngineMockKuCoin, boost::shared_ptr<TDEngineMockKuCoin> >("Engine")
            .def(init<>())
            .def("init", &TDEngineMockKuCoin::initialize)
            .def("start", &TDEngineMockKuCoin::start)
            .def("stop", &TDEngineMockKuCoin::stop)
            .def("logout", &TDEngineMockKuCoin::logout)
            .def("wait_for_stop", &TDEngineMockKuCoin::wait_for_stop);
}
