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
 * ITDEngine: base class of all trade engine.
 * @Author cjiang (changhao.jiang@taurus.ai)
 * @since   April, 2017
 */

#include "ITDEngine.h"
#include "PageCommStruct.h" /**< REQUEST_ID_RANGE */
#include "Timer.h"
#include "longfist/LFConstants.h"
#include "longfist/LFUtils.h"
#include "longfist/sys_messages.h"
#include <algorithm>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

USING_WC_NAMESPACE

class RidClientManager
{
private:
    /** rid, strategy_name map */
    map<int, string> rid2client;
public:
    inline void set(int rid_start, string name)
    {
        rid2client[rid_start / REQUEST_ID_RANGE] = name;
    }
    inline string get(int rid)
    {
        auto iter = rid2client.find(rid / REQUEST_ID_RANGE);
        if (iter == rid2client.end())
            return "";
        else
            return iter->second;
    }
};

/** manage rid and client_name matching */
RidClientManager rid_manager;
//std::mutex mutex_rid_manager;

ITDEngine::ITDEngine(int source): IEngine(source), default_account_index(-1), local_id(1), request_id(1)
{}

void ITDEngine::set_reader_thread()
{
    reader_thread = ThreadPtr(new std::thread(boost::bind(&ITDEngine::listening, this)));
}
void ITDEngine::req_batch_cancel_orders(const LFBatchCancelOrderField* data, int account_index, int requestId, long rcv_time)
{
    KF_LOG_INFO(logger, "[req_batch_cancel_orders] It's only for td_binancef.");
}
void ITDEngine::req_get_kline_via_rest(const GetKlineViaRest* data, int account_index, int requestId, long rcv_time)
{
    KF_LOG_INFO(logger, "[req_get_kline_via_rest] It's only for td_binancef or td_binance.");
}
void ITDEngine::req_withdraw_currency(const LFWithdrawField* data, int account_index, int requestId)
{
    on_rsp_withdraw(data, requestId,1,"not supported");
}
void ITDEngine::req_transfer_history(const LFTransferHistoryField* data, int account_index, int requestId, bool isWithdraw)
{
    on_rsp_transfer_history(data,isWithdraw,1,request_id,1,"not supported");
}
void ITDEngine::req_inner_transfer(const LFTransferField* data, int account_index, int requestId)
{
    on_rsp_transfer(data, requestId,1,"not supported");
}
/** insert quote */
void ITDEngine::req_quote_insert(const LFInputQuoteField* data, int account_index, int requestId, long rcv_time)
{
    on_rsp_quote_insert(data,requestId,1,"not supported");
}
    /** request cancel quote*/
void ITDEngine::req_quote_action(const LFQuoteActionField* data, int account_index, int requestId, long rcv_time)
{
    on_rsp_quote_action(data,requestId,1,"not supported");
}
void ITDEngine::init()
{
    KF_LOG_INFO(logger, "[user] init: "  );
    reader = yijinjing::JournalReader::createRevisableReader(name());
    KF_LOG_INFO(logger, "[ITDEngine] reader: "<<reader  );
    JournalPair tdPair = getTdJournalPair(source_id);
    KF_LOG_INFO(logger, "[ITDEngine] tdPairfirst: "<<tdPair.first<<"second"<<tdPair.second  );
    writer = yijinjing::JournalSafeWriter::create(tdPair.first, tdPair.second, name());
    KF_LOG_INFO(logger, "[ITDEngine] writer: "<<writer  );
    JournalPair tdSendPair = getTdSendJournalPair(source_id);
    KF_LOG_INFO(logger, "[ITDEngine] tdSendPair: "  );
    send_writer = yijinjing::JournalWriter::create(tdSendPair.first, tdSendPair.second, "SEND_" + name());
    KF_LOG_INFO(logger, "[ITDEngine] send_writer: "<<send_writer  );
    user_helper = TDUserInfoHelperPtr(new TDUserInfoHelper(source_id));
    KF_LOG_INFO(logger, "[ITDEngine] user_helper: "<<user_helper  );
    td_helper = TDEngineInfoHelperPtr(new TDEngineInfoHelper(source_id, name()));
    KF_LOG_INFO(logger, "[ITDEngine] td_helper: "<<td_helper  );
}

void ITDEngine::listening()
{
    yijinjing::FramePtr frame;
    timespec now_time; 
    
    while (isRunning && signal_received < 0)
    {
       // KF_LOG_DEBUG(logger, "Listening started" );
        frame = reader->getNextFrame();
    //    KF_LOG_DEBUG(logger, "[ITDEngine] listening: msg_type=" << msg_type << "  requestId= " << request_id ;
        if (frame.get() != nullptr)
        {
            int msg_type = frame->getMsgType();
            int msg_source = frame->getSource();
            long long temp=cur_time;
            cur_time = frame->getNano();
            temp=cur_time-temp;
            clock_gettime(CLOCK_REALTIME, &now_time);
            KF_LOG_DEBUG(logger, "[ITDEngine::listening] (msg_type) " << msg_type <<" (msg_source) "<< msg_source << " (cur_time) " << cur_time<<" (now_time) "<<now_time.tv_sec<<now_time.tv_nsec <<" sub_time "<<temp<<" request id "<< request_id);
        //   time_t now_time = time(0);

         //   KF_LOG_DEBUG(logger, "[ITDEngine::listening] (msg_type) " << msg_type <<" (msg_source) "<< msg_source << " (cur_time) " << cur_time<<" (now_time) "<<now_time <<" sub_time "<<temp<<" request id "<< request_id);
              if (msg_type == MSG_TYPE_LF_MD)
            {
                void* fdata = frame->getData();
                LFMarketDataField* md = (LFMarketDataField*)fdata;
                on_market_data(md, cur_time);
                continue;
            }
            else if (msg_type < 200)
            {
                // system related...
                if (msg_type == MSG_TYPE_TRADE_ENGINE_LOGIN && msg_source == source_id)
                {
                    try
                    {
                        string content((char*)frame->getData());
                        KF_LOG_INFO(logger, "[user] content: " << content);
                        json j_request = json::parse(content);
                        string client_name = j_request["name"].get<string>();
                        if (add_client(client_name, j_request))
                        {
                            KF_LOG_INFO(logger, "[user] Accepted: " << client_name);
                        }
                        else
                        {
                            KF_LOG_INFO(logger, "[user] Rejected: " << client_name);
                        }
                    }
                    catch (...)
                    {
                        KF_LOG_ERROR(logger, "error in parsing TRADE_ENGINE_LOGIN: " << (char*)frame->getData());
                    }
                }
                else if (msg_type == MSG_TYPE_STRATEGY_END)
                {
                    try
                    {
                        string content((char*)frame->getData());
                        json j_request = json::parse(content);
                        string client_name = j_request["name"].get<string>();
                        if (remove_client(client_name, j_request))
                        {
                            KF_LOG_INFO(logger, "[user] Removed: " << client_name);
                        }
                    }
                    catch (...)
                    {
                        KF_LOG_ERROR(logger, "error in parsing STRATEGY_END: " << (char*)frame->getData());
                    }
                }
                else if(msg_type == MSG_TYPE_STRATEGY_POS_SET && msg_source == source_id)
                {
                    try
                    {
                        string content((char*)frame->getData());
                        json j_request = json::parse(content);
                        string client_name = j_request["name"].get<string>();
                        user_helper->set_pos(client_name, j_request);
                        clients[client_name].pos_handler = PosHandler::create(source_id, content);
                        clients[client_name].pos_handler->set_fee(accounts[clients[client_name].account_index].fee_handler);
                        KF_LOG_DEBUG(logger, "[user] set pos: (client)" << client_name << " (pos)" << clients[client_name].pos_handler->to_string());
                    }
                    catch (...)
                    {
                        KF_LOG_ERROR(logger, "error in parsing STRATEGY_POS_SET: " << (char*)frame->getData());
                    }
                }
                else if (msg_type == MSG_TYPE_TRADE_ENGINE_OPEN && (msg_source <= 0 || msg_source == source_id))
                {
                    on_engine_open();
                }
                else if (msg_type == MSG_TYPE_TRADE_ENGINE_CLOSE && (msg_source <= 0 || msg_source == source_id))
                {
                    on_engine_close();
                }
                else if (msg_type == MSG_TYPE_STRING_COMMAND)
                {
                    string cmd((char*)frame->getData());
                    on_command(cmd);
                }
                else if (msg_type == MSG_TYPE_SWITCH_TRADING_DAY)
                {

                    user_helper->switch_day();
                    for (auto iter: clients)
                    {
                        if (iter.second.pos_handler.get() != nullptr)
                            iter.second.pos_handler->switch_day();
                    }
                    local_id = 1;

                    on_switch_day();
                }
            }
            else if (msg_source == source_id && is_logged_in())
            {
                // from client
                clock_gettime(CLOCK_REALTIME, &now_time);
                KF_LOG_DEBUG(logger, "[ITDEngine::listening] (msg_type) " << msg_type <<" (msg_source) "<< msg_source << " (cur_time) " << cur_time<<" (now_time_insert_0) "<<now_time.tv_sec<<now_time.tv_nsec <<" sub_time "<<temp<<" request id "<< request_id);
                string name = reader->getFrameName();
                auto iter = clients.find(name);
                if (iter == clients.end())
                {
                    KF_LOG_DEBUG(logger, "[ITDEngine::listening] (msg_type)" << msg_type << " (cur_time)" << cur_time << "can not find (name)" << name);
                    continue;
                }

                void* fdata = frame->getData();
                int requestId = frame->getRequestId();
                int idx = iter->second.account_index;
                KF_LOG_DEBUG(logger, "[ITDEngine::listening] (msg_type)" << msg_type << " (cur_time)" << cur_time << " (requestId)" << requestId << " (name)" << name << " (idx)" << idx);
                switch (msg_type)
                {
                    case MSG_TYPE_LF_QRY_POS:
                    {
                        LFQryPositionField* pos = (LFQryPositionField*)fdata;
                        //strcpy(pos->BrokerID, accounts[idx].BrokerID);
                        //strcpy(pos->InvestorID, accounts[idx].InvestorID);
                        req_investor_position(pos, idx, requestId);
                        break;
                    }
                    case MSG_TYPE_LF_ORDER:
                    {
                        //clock_gettime(CLOCK_REALTIME, &now_time);
                        //KF_LOG_DEBUG(logger, "[ITDEngine::listening] (msg_type) " << msg_type <<" (msg_source) "<< msg_source << " (cur_time) " << cur_time<<" (now_time_insert_1) "<<now_time.tv_sec<<now_time.tv_nsec <<" sub_time "<<temp<<" request id "<< request_id);
                        LFInputOrderField* order = (LFInputOrderField*)fdata;
                        strcpy(order->BrokerID, accounts[idx].BrokerID);
                        strcpy(order->InvestorID, accounts[idx].InvestorID);
                        strcpy(order->UserID, accounts[idx].UserID);
                        //strcpy(order->BusinessUnit, accounts[idx].BusinessUnit);
                        string order_ref = std::to_string(local_id);
                        //通过orderRef来记录requestId(KfOrderID), InstrumentID
                        td_helper->record_order(local_id, requestId);
                        user_helper->record_order(name, local_id, requestId, order->InstrumentID);
                        local_id ++;
                        strcpy(order->OrderRef, order_ref.c_str());
                        long before_nano = kungfu::yijinjing::getNanoTime();
                        //clock_gettime(CLOCK_REALTIME, &now_time);
                        //KF_LOG_DEBUG(logger, "[ITDEngine::listening] (msg_type) " << msg_type <<" (msg_source) "<< msg_source << " (cur_time) " << cur_time<<" (now_time_insert_2) "<<now_time.tv_sec<<now_time.tv_nsec <<" sub_time "<<temp<<" request id "<< request_id);
                        req_order_insert(order, idx, requestId, cur_time);
                        //clock_gettime(CLOCK_REALTIME, &now_time);
                        //KF_LOG_DEBUG(logger, "[ITDEngine::listening] (msg_type) " << msg_type <<" (msg_source) "<< msg_source << " (cur_time) " << cur_time<<" (now_time_insert_3) "<<now_time.tv_sec<<now_time.tv_nsec <<" sub_time "<<temp<<" request id "<< request_id);
                       // KF_LOG_DEBUG(logger, "[cancel_order] (rid)" << order_id << " (ticker)" << order->InstrumentID << " (ref)" << order_ref << "(local_id_order_action)" << local_id_order_action);
                        // insert order, we need to track in send
                        send_writer->write_frame_extra(order, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER, 1/*ISLAST*/, requestId, before_nano);
                        KF_LOG_INFO(logger, "[insert_order] (rid)" << requestId << " (ticker)" << order->InstrumentID << " (ref)" << order_ref);
                        break;
                    }
                    case MSG_TYPE_LF_QUOTE:
                    {
                        LFInputQuoteField* quote = (LFInputQuoteField*)fdata;
                        //strcpy(order->BusinessUnit, accounts[idx].BusinessUnit);
                        string order_ref = std::to_string(local_id);
                        //通过orderRef来记录requestId(KfOrderID), InstrumentID
                        td_helper->record_order(local_id, requestId);
                        user_helper->record_order(name, local_id, requestId, quote->InstrumentID);
                        local_id++;
                        strcpy(quote->OrderRef, order_ref.c_str());
                        long before_nano = kungfu::yijinjing::getNanoTime();
                        req_quote_insert(quote, idx, requestId, cur_time);
                        // insert order, we need to track in send
                        send_writer->write_frame_extra(quote, sizeof(LFInputQuoteField), source_id, MSG_TYPE_LF_QUOTE, 1/*ISLAST*/, requestId, before_nano);
                        KF_LOG_INFO(logger, "[insert_quote] (rid)" << requestId << " (ticker)" << quote->InstrumentID << " (ref)" << order_ref);
                        break;
                    }
                    case MSG_TYPE_LF_ORDER_ACTION:
                    {
                        LFOrderActionField* order = (LFOrderActionField*)fdata;
                        strcpy(order->BrokerID, accounts[idx].BrokerID);
                        strcpy(order->InvestorID, accounts[idx].InvestorID);
                        int order_id = order->KfOrderID;
                        int local_id_order_action = 0;
                        //通过KfOrderID来查找原始的 orderRef, InstrumentID
                        if (user_helper->get_order(name, order_id, local_id_order_action, order->InstrumentID))
                        {
                            string order_ref = std::to_string(local_id_order_action);
                            strcpy(order->OrderRef, order_ref.c_str());
                            KF_LOG_DEBUG(logger, "[cancel_order] (rid)" << order_id << " (ticker)" << order->InstrumentID << " (ref)" << order_ref << "(local_id_order_action)" << local_id_order_action);
                            req_order_action(order, idx, requestId, cur_time);
                            break;
                        }
                        KF_LOG_DEBUG(logger, "[cancel_order] can not find orderRef by (rid)" << order_id << " (ticker)" << order->InstrumentID);
                    }
                    case MSG_TYPE_LF_BATCH_CANCEL_ORDER:
                    {
                        LFBatchCancelOrderField* order = (LFBatchCancelOrderField*)fdata;
                        strcpy(order->BrokerID, accounts[idx].BrokerID);
                        strcpy(order->InvestorID, accounts[idx].InvestorID);
                        /*
                        //通过KfOrderID来查找原始的 orderRef, InstrumentID
                        for (int i = 0; i < order->SizeOfList; i++) {
                            int local_id_order_action = 0;
                            if (user_helper->get_order(name, order->InfoList[i].KfOrderID, local_id_order_action, order->InstrumentID)) {
                                string order_ref = std::to_string(local_id_order_action);
                                strcpy(order->InfoList[i].OrderRef, order_ref.c_str());
                                KF_LOG_DEBUG(logger, "[cancel_order] (rid)" << order->InfoList[i].KfOrderID << " (ticker)" << order->InstrumentID << " (ref)" << order_ref << "(local_id_order_action)" << local_id_order_action);
                            }
                            else
                                KF_LOG_DEBUG(logger, "[req_batch_cancel_orders] can not find orderRef by (rid)" << order->InfoList[i].KfOrderID << " (ticker)" << order->InstrumentID);
                        }
                        */

                        vector<LFBatchCancelOrderField*> order_vec;
                        //通过KfOrderID来查找原始的 orderRef, InstrumentID
                        for (int i = 0; i < order->SizeOfList; i++) {
                            string order_ref;
                            char ticker_c[31];
                            memset(ticker_c, 0, 31);
                            int local_id_order_action = 0;
                            bool find_in_order_vec = false;

                            if (user_helper->get_order(name, order->InfoList[i].KfOrderID, local_id_order_action, ticker_c)) {
                                order_ref = std::to_string(local_id_order_action);
                                //strcpy(order->InfoList[i].OrderRef, order_ref.c_str());
                                KF_LOG_DEBUG(logger, "[cancel_order] (rid)" << order->InfoList[i].KfOrderID << " (ticker)" << order->InstrumentID << " (ref)" << order_ref << "(local_id_order_action)" << local_id_order_action);
                            }
                            else
                                KF_LOG_DEBUG(logger, "[req_batch_cancel_orders] can not find orderRef by (rid)" << order->InfoList[i].KfOrderID << " (ticker)" << order->InstrumentID);
                            
                            if (order_vec.size() != 0) {
                                for (auto itr : order_vec) {
                                    if (strcmp(itr->InstrumentID, ticker_c) == 0) {
                                        itr->InfoList[itr->SizeOfList].KfOrderID = order->InfoList[i].KfOrderID;
                                        itr->InfoList[itr->SizeOfList].ActionFlag = order->InfoList[i].ActionFlag;
                                        strncpy(itr->InfoList[itr->SizeOfList].OrderRef, order_ref.c_str(), 21);
                                        strncpy(itr->InfoList[itr->SizeOfList].OrderSysID, order->InfoList[i].OrderSysID, 31);
                                        strncpy(itr->InfoList[itr->SizeOfList].MiscInfo, order->InfoList[i].MiscInfo, 64);
                                        itr->SizeOfList++;
                                        find_in_order_vec = true;
                                    }
                                }
                            }

                            if (!find_in_order_vec) {
                                LFBatchCancelOrderField* new_data = new LFBatchCancelOrderField;

                                memset(new_data, 0, sizeof(LFBatchCancelOrderField));
                                strncpy(new_data->BrokerID, order->BrokerID, sizeof(new_data->BrokerID));
                                strncpy(new_data->InvestorID, order->InvestorID, sizeof(new_data->InvestorID));
                                strncpy(new_data->InstrumentID, ticker_c, sizeof(new_data->InstrumentID));
                                strncpy(new_data->ExchangeID, order->ExchangeID, sizeof(new_data->ExchangeID));
                                strncpy(new_data->UserID, order->UserID, sizeof(new_data->UserID));
                                new_data->RequestID = order->RequestID;
                                new_data->SizeOfList = 0;

                                new_data->InfoList[new_data->SizeOfList].KfOrderID = order->InfoList[i].KfOrderID;
                                new_data->InfoList[new_data->SizeOfList].ActionFlag = order->InfoList[i].ActionFlag;
                                strncpy(new_data->InfoList[new_data->SizeOfList].OrderRef, order_ref.c_str(), 21);
                                strncpy(new_data->InfoList[new_data->SizeOfList].OrderSysID, order->InfoList[i].OrderSysID, 31);
                                strncpy(new_data->InfoList[new_data->SizeOfList].MiscInfo, order->InfoList[i].MiscInfo, 64);
                                new_data->SizeOfList++;

                                order_vec.push_back(new_data);
                            }
                        }

                        for (auto itr : order_vec) {
                            req_batch_cancel_orders(itr, idx, requestId, cur_time);
                        }
                        for (auto itr : order_vec) {
                            free(itr);
                        }
                        break;
                    }
                    case MSG_TYPE_LF_QUOTE_ACTION:
                    {
                        LFQuoteActionField* order = (LFQuoteActionField*)fdata;
                        int order_id = order->KfOrderID;
                        int local_id_order_action = 0;
                        //通过KfOrderID来查找原始的 orderRef, InstrumentID
                        if (user_helper->get_order(name, order_id, local_id_order_action, order->InstrumentID))
                        {
                            string order_ref = std::to_string(local_id_order_action);
                            strcpy(order->OrderRef, order_ref.c_str());
                            KF_LOG_DEBUG(logger, "[cancel_quote] (rid)" << order_id << " (ticker)" << order->InstrumentID << " (ref)" << order_ref << "(local_id_order_action)" << local_id_order_action);
                            req_quote_action(order, idx, requestId, cur_time);
                            break;
                        }
                        else
                        {
                            KF_LOG_DEBUG(logger, "[accept_quote] (quote id)" << order->QuoteID );
                            req_quote_action(order, idx, requestId, cur_time);
                        }
                    }
                    case MSG_TYPE_LF_QRY_ACCOUNT:
                    {
                        LFQryAccountField* acc = (LFQryAccountField*)fdata;
                        strcpy(acc->BrokerID, accounts[idx].BrokerID);
                        strcpy(acc->InvestorID, accounts[idx].InvestorID);
                        req_qry_account(acc, idx, requestId);
                        break;
                    }
                    case MSG_TYPE_LF_WITHDRAW:
                    {
                        LFWithdrawField* withdraw = (LFWithdrawField*)fdata;
                        req_withdraw_currency(withdraw, idx, requestId);
                        KF_LOG_DEBUG(logger, "[withdraw_currency] (rid)" << requestId << " (currency) " 
                            << withdraw->Currency << " (volume) " << withdraw->Volume
                            <<" (address) "<<withdraw->Address<<" (tag) "<<withdraw->Tag);
                        break;
                    }
                    case MSG_TYPE_LF_INNER_TRANSFER:
                    {
                        LFTransferField* transfer = (LFTransferField*)fdata;
                        req_inner_transfer(transfer, idx, requestId);
                        KF_LOG_DEBUG(logger, "[req_inner_transfer] (rid)" << requestId << " (currency) " 
                            << transfer->Currency << " (volume) " << transfer->Volume
                            <<" (from) "<<transfer->From<<" (from_name) "<<transfer->FromName 
                            <<" (to) "<<transfer->To<<" (to_name) "<<transfer->ToName );
                        break;
                    }
                    case MSG_TYPE_LF_TRANSFER_HISTORY:
                    {
                        LFTransferHistoryField *detransfer = (LFTransferHistoryField*)fdata;
                        req_transfer_history(detransfer, idx, requestId, detransfer->IsWithdraw);       
                        KF_LOG_DEBUG(logger, "[transfer_history] (rid)" << requestId );
                        break;
                    }
                    case MSG_TYPE_LF_GET_KLINE_VIA_REST:
                    {
                        GetKlineViaRest* req = (GetKlineViaRest*)fdata;
                        req_get_kline_via_rest(req, idx, requestId, cur_time);
                        KF_LOG_DEBUG(logger, "[GetKlineViaRest] (rid)" << requestId);
                        break;
                    }
                    
                    default:
                        KF_LOG_DEBUG(logger, "[Unexpected] frame found: (msg_type)" << msg_type << ", (name)" << name);
                }
            }
        }
       // KF_LOG_DEBUG(logger, "Listening end"<< isRunning <<"received"<<signal_received);
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

static const LFRspPositionField empty_pos = {};

void ITDEngine::write_errormsg(int errorid,string errormsg)
{
    LFErrorMsgField error;
    strcpy(error.Name,get_module_name().data());
    strcpy(error.Type,"td");
    error.ErrorId = errorid;
    strcpy(error.ErrorMsg,errormsg.data());
    LFErrorMsgField* errorPtr = &error;
    KF_LOG_DEBUG(logger, "TD write begin");
    writer->write_frame(errorPtr, sizeof(LFErrorMsgField), source_id, MSG_TYPE_LF_ERRORMSG, 1/*islast*/, -1/*invalidRid*/);
    KF_LOG_DEBUG(logger, "TD write end");
}

/** on investor position, engine (on_data) */
void ITDEngine::on_rsp_position(const LFRspPositionField* pos, bool isLast, int requestId, int errorId, const char* errorMsg)
{
    if (errorId == 0)
    {
        writer->write_frame(pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS, isLast, requestId);
        KF_LOG_DEBUG(logger, "[RspPosition]" << " (rid)" << requestId
                                             << " (ticker)" << pos->InstrumentID
                                             << " (dir)" << pos->PosiDirection
                                             << " (cost)" << pos->PositionCost
                                             << " (pos)" << pos->Position
                                             << " (yd)" << pos->YdPosition);
    }
    else
    {
        writer->write_error_frame(pos, sizeof(LFRspPositionField), source_id, MSG_TYPE_LF_RSP_POS, isLast, requestId, errorId, errorMsg);
        KF_LOG_ERROR(logger, "[RspPosition] fail! " << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg)" << errorMsg);
    }
}
/** on rsp order insert, engine (on_data) */
void ITDEngine::on_rsp_order_insert(const LFInputOrderField* order, int requestId, int errorId, const char* errorMsg)
{
    if (errorId == 0)
    {
        writer->write_frame(order, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER, true, requestId);
        KF_LOG_DEBUG(logger, "[RspOrder]" << " (rid)" << requestId
                                          << " (ticker)" << order->InstrumentID);
    }
    else
    {
        string name = rid_manager.get(requestId);
        user_helper->set_order_status(name, requestId, LF_CHAR_Error);
        writer->write_error_frame(order, sizeof(LFInputOrderField), source_id, MSG_TYPE_LF_ORDER, true, requestId, errorId, errorMsg);
        KF_LOG_ERROR(logger, "[RspOrder] fail!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg)" << errorMsg);
    }
}
/** on rsp quote insert, engine (on_data) */
void ITDEngine::on_rsp_quote_insert(const LFInputQuoteField* order, int requestId, int errorId, const char* errorMsg)
{
    if (errorId == 0)
    {
        writer->write_frame(order, sizeof(LFInputQuoteField), source_id, MSG_TYPE_LF_QUOTE, true, requestId);
        KF_LOG_DEBUG(logger, "[RspQuote]" << " (rid)" << requestId
                                          << " (ticker)" << order->InstrumentID);
    }
    else
    {
        writer->write_error_frame(order, sizeof(LFInputQuoteField), source_id, MSG_TYPE_LF_QUOTE, true, requestId, errorId, errorMsg);
        KF_LOG_ERROR(logger, "[RspQuote] fail!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg)" << errorMsg);
    }
}
/** on rsp order action, engine (on_data) */
void ITDEngine::on_rsp_order_action(const LFOrderActionField *action, int requestId, int errorId, const char *errorMsg)
{
    if (errorId == 0)
    {
        writer->write_frame(action, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION, true, requestId);
        KF_LOG_DEBUG(logger, "[RspAction]" << " (rid)" << requestId << " (ticker)" << action->InstrumentID);
    }
    else
    {
        writer->write_error_frame(action, sizeof(LFOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION, true, requestId, errorId, errorMsg);
        KF_LOG_ERROR(logger, "[RspAction] fail!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg)" << errorMsg);
    }
}
void ITDEngine::on_rsp_quote_action(const LFQuoteActionField *action, int requestId, int errorId, const char *errorMsg)
{
    if (errorId == 0)
    {
        writer->write_frame(action, sizeof(LFQuoteActionField), source_id, MSG_TYPE_LF_QUOTE_ACTION, true, requestId);
        KF_LOG_DEBUG(logger, "[RspQuoteAction]" << " (rid)" << requestId << " (ticker)" << action->InstrumentID);
    }
    else
    {
        writer->write_error_frame(action, sizeof(LFQuoteActionField), source_id, MSG_TYPE_LF_QUOTE_ACTION, true, requestId, errorId, errorMsg);
        KF_LOG_ERROR(logger, "[RspQuoteAction] fail!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg)" << errorMsg);
    }
}
void ITDEngine::on_rsp_withdraw(const LFWithdrawField* data, int requestId,int errorId, const char* errorMsg){
    if (errorId == 0)
    {
        writer->write_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW, true, requestId);
        KF_LOG_DEBUG(logger, "[RspWithdraw]" << " (rid)" << requestId << " (currency) " << data->Currency
            << " (volume) " << data->Volume << " (address) " << data->Address << " (tag) " << data->Tag);
    }
    else
    {
        writer->write_error_frame(data, sizeof(LFWithdrawField), source_id, MSG_TYPE_LF_WITHDRAW, true, requestId, errorId, errorMsg);
        KF_LOG_ERROR(logger, "[RspWithdraw] fail!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg)" << errorMsg);
    }
}

void ITDEngine::on_rsp_transfer(const LFTransferField* data, int requestId,int errorId, const char* errorMsg){
    if (errorId == 0)
    {
        writer->write_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER, true, requestId);
        KF_LOG_DEBUG(logger, "[RspTransfer]" << " (rid)" << requestId << " (currency) " << data->Currency
            << " (volume) " << data->Volume << " (From) " << data->From << " (To) " << data->To);
    }
    else
    {
        writer->write_error_frame(data, sizeof(LFTransferField), source_id, MSG_TYPE_LF_INNER_TRANSFER, true, requestId, errorId, errorMsg);
        KF_LOG_ERROR(logger, "[RspTransfer] fail!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg)" << errorMsg);
    }
}

void ITDEngine::on_rsp_transfer_history(const LFTransferHistoryField* data, bool is_withdraw, bool isLast, int requestId,int errorId, const char* errorMsg)
{
    LFTransferHistoryField* p = const_cast<LFTransferHistoryField*>(data);
    p->IsWithdraw = is_withdraw;
    if (errorId == 0)
    {
        writer->write_frame(p, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY, isLast, requestId);
        KF_LOG_DEBUG(logger, "[Rsp_Transfer_History]" << " (rid)" << requestId << " (isWithdraw)" << data->IsWithdraw 
                                                      << " (insertTime)" << data->TimeStamp
                                                      << " (amount)" << data->Volume
                                                      << " (asset)" << data->Currency
                                                      << " (address)" << data->Address
                                                      << " (addressTag)" << data->Tag
                                                      << " (txId)" << data->TxId
                                                      << " (status)" << data->Status);
        
    }
    else
    {
        writer->write_error_frame(data, sizeof(LFTransferHistoryField), source_id, MSG_TYPE_LF_TRANSFER_HISTORY, isLast, requestId, errorId, errorMsg);
            KF_LOG_ERROR(logger, "[Rsp_Transfer_History] fail!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg)" << errorMsg);
    }
} 

/** on rsp account info, engine (on_data) */
void ITDEngine::on_rsp_account(const LFRspAccountField* account, bool isLast, int requestId, int errorId, const char* errorMsg)
{
    if (errorId == 0)
    {
        writer->write_frame(account, sizeof(LFRspAccountField), source_id, MSG_TYPE_LF_RSP_ACCOUNT, isLast, requestId);
        KF_LOG_DEBUG(logger, "[RspAccount]" << " (rid)" << requestId
                                            << " (investor)" << account->InvestorID
                                            << " (balance)" << account->Balance
                                            << " (value)" << account->MarketValue);
    }
    else
    {
        writer->write_error_frame(account, sizeof(LFRspAccountField), source_id, MSG_TYPE_LF_RSP_ACCOUNT, isLast, requestId, errorId, errorMsg);
        KF_LOG_ERROR(logger, "[RspAccount] fail!" << " (rid)" << requestId << " (errorId)" << errorId << " (errorMsg)" << errorMsg);
    }
}
void ITDEngine::on_rtn_quote(const LFRtnQuoteField* rtn_quote)
{
    writer->write_frame(rtn_quote, sizeof(LFRtnQuoteField), source_id, MSG_TYPE_LF_RTN_QUOTE, 1/*islast*/, rtn_quote->RequestID);
    KF_LOG_DEBUG_FMT(logger, "[quote] (id)%d (ref)%s (ticker)%s (quote id)%lld (Price)%lld (St)%c",
                     rtn_quote->RequestID,
                     rtn_quote->OrderRef,
                     rtn_quote->InstrumentID,
                     rtn_quote->ID,
                     rtn_quote->Price,
                     rtn_quote->OrderStatus);
}
/** on rtn order, engine (on_data) */
void ITDEngine::on_rtn_order(const LFRtnOrderField* rtn_order)
{
    int local_id = std::stoi(string(rtn_order->OrderRef));
    int rid = td_helper->get_order_id(local_id);
    writer->write_frame(rtn_order, sizeof(LFRtnOrderField), source_id, MSG_TYPE_LF_RTN_ORDER, 1/*islast*/, rid);
    KF_LOG_DEBUG_FMT(logger, "[o] (id)%d (ref)%s (ticker)%s (Vsum)%llu (Vtrd)%llu (Vrmn)%llu (Price)%llu (St)%c",
                     rid,
                     rtn_order->OrderRef,
                     rtn_order->InstrumentID,
                     rtn_order->VolumeTotalOriginal,
                     rtn_order->VolumeTraded,
                     rtn_order->VolumeTotal,
                     rtn_order->LimitPrice,
                     rtn_order->OrderStatus);
    string name = rid_manager.get(rid);
    user_helper->set_order_status(name, rid, rtn_order->OrderStatus);
    td_helper->set_order_status(local_id, rtn_order->OrderStatus);
}
/** on rtn trade, engine (on_data) */
void ITDEngine::on_rtn_trade(const LFRtnTradeField* rtn_trade)
{
    int local_id = std::stoi(string(rtn_trade->OrderRef));
    int rid = td_helper->get_order_id(local_id);
    writer->write_frame(rtn_trade, sizeof(LFRtnTradeField), source_id, MSG_TYPE_LF_RTN_TRADE, 1/*islast*/, rid);
    KF_LOG_DEBUG_FMT(logger, "[t] (id)%d (ref)%s (ticker)%s (V)%llu (P)%lld (D)%c",
                     rid,
                     rtn_trade->OrderRef,
                     rtn_trade->InstrumentID,
                     rtn_trade->Volume,
                     rtn_trade->Price,
                     rtn_trade->Direction);
    string name = rid_manager.get(rid);
    auto iter = clients.find(name);
    if (iter != clients.end() && iter->second.pos_handler.get() != nullptr)
    {
        iter->second.pos_handler->update(rtn_trade);
        KF_LOG_DEBUG(logger, "[cost]"
                << " (long_amt)" << iter->second.pos_handler->get_long_balance(rtn_trade->InstrumentID)
                << " (long_fee)" << iter->second.pos_handler->get_long_fee(rtn_trade->InstrumentID)
                << " (short_amt)" << iter->second.pos_handler->get_short_balance(rtn_trade->InstrumentID)
                << " (short_fee)" << iter->second.pos_handler->get_short_fee(rtn_trade->InstrumentID)
        );
    }
}
/** get serial of bar via restful api (only for binance and binancef) */
void ITDEngine::on_bar_serial1000(const LFBarSerial1000Field* data, int rid)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFBarSerial1000Field), source_id, MSG_TYPE_LF_BAR_SERIAL1000, 1/*islast*/, rid);
        KF_LOG_DEBUG(logger, "LFBarSerial1000Field:"
            << " (InstrumentID)" << data->InstrumentID
            << " (ExchangeID)" << data->ExchangeID
            << " (BarLevel)" << data->BarLevel
        );
    }
}

bool ITDEngine::add_client(const string& client_name, const json& j_request)
{
    string folder = j_request["folder"].get<string>();
    int rid_s = j_request["rid_s"].get<int>();
    int rid_e = j_request["rid_e"].get<int>();
    long last_switch_day = j_request["last_switch_nano"].get<long>();

    KF_LOG_DEBUG(logger, "[add_client]" << "(rid_s)" << rid_s << " (rid_e)" << rid_e << " (last_switch_day)" << last_switch_day);
    rid_manager.set(rid_s, client_name);
    user_helper->load(client_name);
    auto iter = clients.find(client_name);
    if (iter == clients.end())
    {
        if (default_account_index < 0)
        {
            return false;
        }
        else
        {
            int idx = reader->addJournal(folder, client_name);

            KF_LOG_DEBUG(logger, "[add_client]" << "(rid_s)" << rid_s << " (rid_e)" << rid_e << " (last_switch_day)" << last_switch_day << " (addJournal.idx)" << idx);

            reader->seekTimeJournal(idx, cur_time);
            ClientInfoUnit& status = clients[client_name];
            status.is_alive = true;
            status.journal_index = idx;
            status.account_index = default_account_index;
            status.rid_start = rid_s;
            status.rid_end = rid_e;
        }
    }
    else if (iter->second.is_alive)
    {
        KF_LOG_ERROR(logger, "login already exists... (info)" << client_name);
    }
    else
    {
        if (iter->second.journal_index < 0)
            iter->second.journal_index = reader->addJournal(folder, client_name);

        reader->seekTimeJournal(iter->second.journal_index, cur_time);
        iter->second.rid_start = rid_s;
        iter->second.rid_end = rid_e;
        iter->second.is_alive = true;
    }
    /**
     * json_ack: {'name': 'bl_test', 'Pos':{'ic1701':[100,50,0,0,0,0]}, 'Source': 1, 'ok': true}
     * failed: {'name': 'bl_test', 'ok': false}
     */
    json json_ack = user_helper->get_pos(client_name);
    json_ack[PH_FEE_SETUP_KEY] = accounts[clients[client_name].account_index].fee_handler->to_json();
    if (json_ack["ok"].get<bool>())
    {
        PosHandlerPtr pos_handler = PosHandler::create(source_id, json_ack);
        clients[client_name].pos_handler = pos_handler;
        if (json_ack.find("nano") != json_ack.end() && json_ack["nano"].get<long>() < last_switch_day)
        {
            pos_handler->switch_day();
            json_ack["Pos"] = pos_handler->to_json()["Pos"];
        }
    }
    string json_content = json_ack.dump();
    writer->write_frame(json_content.c_str(), json_content.length() + 1, source_id, MSG_TYPE_TRADE_ENGINE_ACK, 1, -1);
    return true;
}

bool ITDEngine::remove_client(const string &client_name, const json &j_request)
{
    auto iter = clients.find(client_name);
    if (iter == clients.end())
        return false;
    reader->expireJournal(iter->second.journal_index);
    iter->second.is_alive = false;
    if (iter->second.pos_handler.get() != nullptr)
    {
        json j_pos = iter->second.pos_handler->to_json();
        user_helper->set_pos(client_name, j_pos);
        iter->second.pos_handler.reset();
    }
    if (is_logged_in() && cancel_all_on_strategy_disconnect)
    {
        // cancel all pending orders, and clear the memory
        auto orders = user_helper->get_existing_orders(client_name);
        int idx = iter->second.account_index;
        for (int order_id: orders)
        {
            LFOrderActionField action = {};
            action.ActionFlag = LF_CHAR_Delete;
            action.KfOrderID = order_id;
            action.LimitPrice = 0;
            action.VolumeChange = 0;
            strcpy(action.BrokerID, accounts[idx].BrokerID);
            strcpy(action.InvestorID, accounts[idx].InvestorID);
            int local_id_remove_client = 0;
            if (user_helper->get_order(client_name, order_id, local_id_remove_client, action.InstrumentID))
            {
                string order_ref = std::to_string(local_id_remove_client);
                strcpy(action.OrderRef, order_ref.c_str());
                KF_LOG_DEBUG(logger, "[cancel_remain_order] (rid)" << order_id << " (ticker)" << action.InstrumentID << " (ref)" << order_ref << " (local_id_remove_client)" << local_id_remove_client);
                req_order_action(&action, iter->second.account_index, order_id, cur_time);
            }
        }
    }
    else if (!cancel_all_on_strategy_disconnect) {
        KF_LOG_INFO(logger, "[remove_client] cancel_all_on_strategy_disconnect == false, won't cancel all order while removing strategy");
    }
    user_helper->remove(client_name);
    return true;
}

void ITDEngine::load(const json& j_config)
{
    if (j_config.find(PH_FEE_SETUP_KEY) != j_config.end())
    {
        default_fee_handler = FeeHandlerPtr(new FeeHandler(j_config[PH_FEE_SETUP_KEY]));
    }
    auto iter = j_config.find("accounts");
    if (iter != j_config.end())
    {
        int account_num = iter.value().size();
        KF_LOG_INFO(logger, "[account] number: " << account_num);
        // ITDEngine's resize
        accounts.resize(account_num);
        // base class resize account info structures.
        resize_accounts(account_num);
        int account_idx = 0;
        for (auto& j_account: iter.value())
        {
            KF_LOG_INFO(logger, "account info ");
            const json& j_info = j_account["info"];
            accounts[account_idx] = load_account(account_idx, j_info);
            auto &fee_handler = accounts[account_idx].fee_handler;
            KF_LOG_INFO(logger, "PH_FEE_SETUP_KEY if else ");
            if (fee_handler.get() == nullptr)
            {
                if (j_info.find(PH_FEE_SETUP_KEY) != j_info.end())
                {
                    fee_handler = FeeHandlerPtr(new FeeHandler(j_info[PH_FEE_SETUP_KEY]));
                }
                else
                {
                    if (default_fee_handler.get() == nullptr)
                    {
                        KF_LOG_ERROR(logger, "[client] no fee_handler (idx)" << account_idx);
                        throw std::runtime_error("cannot find fee_handler for account!");
                    }
                    else
                    {
                        fee_handler = default_fee_handler;
                    }
                }
            }
            /** parse client */
            const json& j_clients = j_account["clients"];
            sendMessagetoMonitor(j_config,j_clients);
            for (auto& j_client: j_clients)
            {
                string client_name = j_client.get<string>();
                ClientInfoUnit& client_status = clients[client_name];
                client_status.account_index = account_idx;
                client_status.is_alive = false;
                client_status.journal_index = -1;
                KF_LOG_INFO(logger, "[client] (name)" << client_name << " (account)" << accounts[client_status.account_index].InvestorID);
            }
            /** set default */
            if (j_account["is_default"].get<bool>())
                default_account_index = account_idx;
            // add
            account_idx ++;
        }
    }
}

void ITDEngine::sendMessagetoMonitor(const json& j_config, const json& j_clients)
{
    KF_LOG_INFO(logger, "sendMessagetoMonitor ");
    if(j_config.find("name") == j_config.end() || j_config.find("monitor_url") == j_config.end())
    {
        return;
    }
    std::string name = j_config["name"].get<std::string>();
    std::string monitor_url = j_config["monitor_url"].get<std::string>();
    std::vector<std::string> results {};
    std::vector<std::string> sts;
    for (auto& client : j_clients)
        sts.push_back(client.get<std::string>());
    boost::split(results, name, boost::is_any_of("_"));
    if (results.size() != 2)
    {
        KF_LOG_INFO(logger, "parse name error,must be xxx_xxx,but is " << name);
        return ;
    }
    else
    {
        KF_LOG_INFO(logger, "sendMessagetoMonitor :" <<results[0] << "," << results[1]);
    }
    
    if (!m_monitorClient->td_strategy(results[1], results[0], sts))
    {
        KF_LOG_INFO(logger, "td_strategy to monitor error,name@" << name << ",url@" << monitor_url);
        return;
    }
}

TradeAccount ITDEngine::load_account(int idx, const json& j_account)
{
    KF_LOG_ERROR(logger, "[account] NOT IMPLEMENTED! (content)" << j_account);
    throw std::runtime_error("load_account not implemented yet!");
}

bool ITDEngine::is_post_only(const LFInputOrderField* data)
{
    //if(data != nullptr && data->MiscInfo[39] == '1')
    if(data != nullptr && data->MiscInfo[43] == '1')
    {
        return true;
    }
    else 
    {
        return false;
    }
}
