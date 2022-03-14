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

//
// Created by cjiang on 17/4/11.
//

#include "TDEngineCTP.h"
#include "longfist/ctp.h"
#include "longfist/LFUtils.h"
#include "TypeConvert.hpp"
#include <stdlib.h>
#include <cstdlib>  
#include <string.h>
#include <boost/algorithm/string.hpp>

USING_WC_NAMESPACE

TDEngineCTP::TDEngineCTP(): ITDEngine(SOURCE_CTP),need_settleConfirm(true), need_authenticate(false), curAccountIdx(-1)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.CTP");
    KF_LOG_INFO(logger, "[ATTENTION] default to confirm settlement and no authentication!");
}

void TDEngineCTP::init()
{
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
}

void TDEngineCTP::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "TDEngineCTP::pre_load");
    front_uri = j_config["WC_CONFIG_KEY_FRONT_URI"].get<string>();
    if (j_config.find("WC_CONFIG_KEY_NEED_SETTLE_CONFIRM") != j_config.end()
        && !j_config["WC_CONFIG_KEY_NEED_SETTLE_CONFIRM"].get<bool>())
    {
        need_settleConfirm = false;
        KF_LOG_INFO(logger, "[ATTENTION] no need to confirm settlement!");
    }
    if (j_config.find("WC_CONFIG_KEY_NEED_AUTH") != j_config.end()
        && j_config["WC_CONFIG_KEY_NEED_AUTH"].get<bool>())
    {
        need_authenticate = true;
        KF_LOG_INFO(logger, "[ATTENTION] need to authenticate code!");
    }
}

void TDEngineCTP::resize_accounts(int account_num)
{
    account_units.resize(account_num);
}

TradeAccount TDEngineCTP::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "TDEngineCTP::load_account");
    // internal load

    //from demo : strncpy(qryInvestor.InvestorID, g_config.UserID, sizeof(qryInvestor.InvestorID));
    string broker_id = j_config["WC_CONFIG_KEY_BROKER_ID"].get<string>();
    string user_id = j_config["WC_CONFIG_KEY_USER_ID"].get<string>();
    string investor_id = j_config["WC_CONFIG_KEY_INVESTOR_ID"].get<string>();
    string password = j_config["WC_CONFIG_KEY_PASSWORD"].get<string>();

    AccountUnitCTP& unit = account_units[idx];
    unit.api = nullptr;
    unit.front_id = -1;
    unit.session_id = -1;
    unit.initialized = false;
    unit.connected = false;
    unit.authenticated = false;
    unit.logged_in = false;
    unit.settle_confirmed = false;
    if (need_authenticate) {
        unit.auth_code = j_config["WC_CONFIG_KEY_AUTH_CODE"].get<string>();
        unit.app_id = j_config["WC_CONFIG_KEY_APP_ID"].get<string>();
        unit.user_product_info = j_config["WC_CONFIG_KEY_USER_PRODUCT_INFO"].get<string>();
    }

    if (j_config.find("markets") != j_config.end()) {
        KF_LOG_INFO(logger, "MDEngineCTP::load markets");
        if (j_config["markets"].is_array())
            for (int i = 0; i < j_config["markets"].size(); i++)
                markets.push_back(j_config["markets"][i]);
        else if (j_config["markets"].is_string())
            markets.push_back(j_config["markets"].get<string>());
    }
    //WhiteLists, not necessary
    if (j_config.find("whiteLists") != j_config.end() && j_config.find("positionWhiteLists") != j_config.end()) {
        unit.coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
        unit.positionWhiteList.ReadWhiteLists(j_config, "positionWhiteLists");
        unit.coinPairWhiteList.Debug_print();
        unit.positionWhiteList.Debug_print();
    }

    // set up
    TradeAccount account = {};
    strncpy(account.BrokerID, broker_id.c_str(), 19);
    strncpy(account.InvestorID, investor_id.c_str(), 19);
    strncpy(account.UserID, user_id.c_str(), 16);
    strncpy(account.Password, password.c_str(), 21);

    //modified 
    accounts[idx] = account;
    KF_LOG_INFO(logger, "TDEngineCTP::load_account try ro connect");
    connect(5000000000);
    //end
    return account;
}

void TDEngineCTP::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "TDEngineCTP::connect");
    for (int idx = 0; idx < account_units.size(); idx ++)
    {
        AccountUnitCTP& unit = account_units[idx];
        if (unit.api == nullptr)
        {
            CThostFtdcTraderApi* api = CThostFtdcTraderApi::CreateFtdcTraderApi();
            if (!api)
            {
                throw std::runtime_error("CTP_TD failed to create api");
            }
            api->RegisterSpi(this);
            unit.api = api;
        }
        if (!unit.connected)
        {
            curAccountIdx = idx;
            unit.api->RegisterFront((char*)front_uri.c_str());
            unit.api->SubscribePublicTopic(THOST_TERT_QUICK); // need check
            unit.api->SubscribePrivateTopic(THOST_TERT_QUICK); // need check
            if (!unit.initialized)
            {
                unit.api->Init();
                unit.initialized = true;
            }
            long start_time = yijinjing::getNanoTime();
            while (!unit.connected && yijinjing::getNanoTime() - start_time < timeout_nsec)
            {}
        }
    }

    //modified
    KF_LOG_INFO(logger, "TDEngineCTP::connect try to login");
    login(5000000000);
    //end
}

void TDEngineCTP::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "TDEngineCTP::login");
    for (int idx = 0; idx < account_units.size(); idx ++)
    {
        KF_LOG_INFO(logger, "TDEngineCTP::login idx" << idx);
        AccountUnitCTP& unit = account_units[idx];
        TradeAccount& account = accounts[idx];
        // authenticate
        if (need_authenticate && !unit.authenticated)
        {
            KF_LOG_INFO(logger, "TDEngineCTP::login, prepare to authenticate");
            struct CThostFtdcReqAuthenticateField req = {0};
            strcpy(req.BrokerID, account.BrokerID);
            strcpy(req.UserID, account.UserID);
            strcpy(req.UserProductInfo, unit.user_product_info.c_str());
            strcpy(req.AppID, unit.app_id.c_str());
            strcpy(req.AuthCode, unit.auth_code.c_str());
            unit.auth_rid = request_id;
            //log for ReqAuthenticate
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.BrokerID " << req.BrokerID);
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.UserID " << req.UserID);
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.UserProductInfo " << req.UserProductInfo);
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.AppID " << req.AppID);
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.AuthCode " << req.AuthCode);
            /*
                0，代表成功;
                -1，表示网络连接失败；
                -2，表示未处理请求超过许可数；
                -3，表示每秒发送请求数超过许可数。
            */
            int iResult = unit.api->ReqAuthenticate(&req, request_id++);
            //if (unit.api->ReqAuthenticate(&req, request_id++))
            if(iResult != 0)
                KF_LOG_ERROR(logger, "[request] auth failed!" << " (Bid)" << req.BrokerID
                                                              << " (Uid)" << req.UserID
                                                              << " (Auth)" << req.AuthCode
                                                              << " (iResult)" << iResult);
            else
                KF_LOG_INFO(logger, "TDEngineCTP::login, ReqAuthenticate iResult: " << iResult);
            long start_time = yijinjing::getNanoTime();
            while (!unit.authenticated && yijinjing::getNanoTime() - start_time < timeout_nsec)
            {}
            KF_LOG_INFO(logger, "TDEngineCTP::login," << " unit.authenticated: " << unit.authenticated);
        }
        // login
        if (!unit.logged_in)
        {
            KF_LOG_INFO(logger, "TDEngineCTP::login, prepare to login");
            struct CThostFtdcReqUserLoginField req = {0};
            strcpy(req.TradingDay, "");
            strcpy(req.UserID, account.UserID);
            strcpy(req.BrokerID, account.BrokerID);
            strcpy(req.Password, account.Password);
            unit.login_rid = request_id;
            //log for ReqUserLogin
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.UserID: " << req.UserID);
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.BrokerID: " << req.BrokerID);
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.Password: " << req.Password);
            KF_LOG_INFO(logger, "TDEngineCTP::login, request_id " << request_id);
            //if (unit.api->ReqUserLogin(&req, request_id++))
            int iResult = unit.api->ReqUserLogin(&req, request_id++);
            if (iResult != 0)
                KF_LOG_ERROR(logger, "[request] login failed!" << " (Bid)" << req.BrokerID
                                                               << " (Uid)" << req.UserID
                                                               << " (iResult)" << iResult);
            else
                KF_LOG_INFO(logger, "TDEngineCTP::login, ReqUserLogin iResult: " << iResult);
            long start_time = yijinjing::getNanoTime();
            while (!unit.logged_in && yijinjing::getNanoTime() - start_time < timeout_nsec)
            {}
            KF_LOG_INFO(logger, "TDEngineCTP::login," << " unit.logged_in: " << unit.logged_in);
        }
        // confirm settlement
        if (need_settleConfirm && !unit.settle_confirmed)
        {
            KF_LOG_INFO(logger, "TDEngineCTP::login, need_settleConfirm && !unit.settle_confirmed");
            struct CThostFtdcSettlementInfoConfirmField req = {0};
            strcpy(req.BrokerID, account.BrokerID);
            strcpy(req.InvestorID, account.InvestorID);
            unit.settle_rid = request_id;
            //log for ReqSettlementInfoConfirm
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.BrokerID: " << req.BrokerID);
            KF_LOG_INFO(logger, "TDEngineCTP::login, req.InvestorID: " << req.InvestorID);
            KF_LOG_INFO(logger, "TDEngineCTP::login, request_id " << request_id);
            //if (unit.api->ReqSettlementInfoConfirm(&req, request_id++))
            int iResult = unit.api->ReqSettlementInfoConfirm(&req, request_id++);
            if (iResult != 0)
                KF_LOG_ERROR(logger, "[request] settlement info failed!" << " (Bid)" << req.BrokerID
                                                                         << " (Iid)" << req.InvestorID
                                                                         << " (iResult)" << iResult);
            else
                KF_LOG_INFO(logger, "TDEngineCTP::login, ReqSettlementInfoConfirm iResult: " << iResult);
            long start_time = yijinjing::getNanoTime();
            while (!unit.settle_confirmed && yijinjing::getNanoTime() - start_time < timeout_nsec)
            {}
            KF_LOG_INFO(logger, "TDEngineCTP::login," << " unit.settle_confirmed: " << unit.settle_confirmed);
        }
    }
}

void TDEngineCTP::logout()
{
    KF_LOG_INFO(logger, "TDEngineCTP::logout");
    for (int idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitCTP& unit = account_units[idx];
        TradeAccount& account = accounts[idx];
        if (unit.logged_in)
        {
            CThostFtdcUserLogoutField req = {};
            strcpy(req.BrokerID, account.BrokerID);
            strcpy(req.UserID, account.UserID);
            unit.login_rid = request_id;
            if (unit.api->ReqUserLogout(&req, request_id++))
            {
                KF_LOG_ERROR(logger, "[request] logout failed!" << " (Bid)" << req.BrokerID
                                                                << " (Uid)" << req.UserID);
            }
        }
        unit.authenticated = false;
        unit.settle_confirmed = false;
        unit.logged_in = false;
    }
}

void TDEngineCTP::release_api()
{
    KF_LOG_INFO(logger, "TDEngineCTP::release_api");
    for (auto& unit: account_units)
    {
        if (unit.api != nullptr)
        {
            unit.api->Release();
            unit.api = nullptr;
        }
        unit.initialized = false;
        unit.connected = false;
        unit.authenticated = false;
        unit.settle_confirmed = false;
        unit.logged_in = false;
        unit.api = nullptr;
    }
}

bool TDEngineCTP::is_logged_in() const
{
    for (auto& unit: account_units)
    {
        if (!unit.logged_in || (need_settleConfirm && !unit.settle_confirmed))
            return false;
    }
    return true;
}

bool TDEngineCTP::is_connected() const
{
    for (auto& unit: account_units)
    {
        if (!unit.connected)
            return false;
    }
    return true;
}

/**
 * req functions
 */
void TDEngineCTP::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    struct CThostFtdcQryInvestorPositionField req = parseTo(*data);
    KF_LOG_DEBUG(logger, "[req_pos]" << " (Bid)" << req.BrokerID
                                     << " (Iid)" << req.InvestorID
                                     << " (Tid)" << req.InstrumentID);

    if (account_units[account_index].api->ReqQryInvestorPosition(&req, requestId))
    {
        KF_LOG_ERROR(logger, "[request] investor position failed!" << " (rid)" << requestId
                                                                   << " (idx)" << account_index);
    }
    send_writer->write_frame(&req, sizeof(CThostFtdcQryInvestorPositionField), source_id, MSG_TYPE_LF_QRY_POS_CTP, 1/*ISLAST*/, requestId);
}

void TDEngineCTP::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    struct CThostFtdcQryTradingAccountField req = parseTo(*data);
    KF_LOG_DEBUG(logger, "[req_account]" << " (Bid)" << req.BrokerID
                                         << " (Iid)" << req.InvestorID);

    if (account_units[account_index].api->ReqQryTradingAccount(&req, requestId))
    {
        KF_LOG_ERROR(logger, "[request] account info failed!" << " (rid)" << requestId
                                                              << " (idx)" << account_index);
    }
    send_writer->write_frame(&req, sizeof(CThostFtdcQryTradingAccountField), source_id, MSG_TYPE_LF_QRY_ACCOUNT_CTP, 1/*ISLAST*/, requestId);
}

void TDEngineCTP::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    struct CThostFtdcInputOrderField req = parseTo(*data);
    req.RequestID = requestId;
    req.IsAutoSuspend = 0;
    req.UserForceClose = 0;
    req.ForceCloseReason = THOST_FTDC_FCC_NotForceClose;
    if (strlen(req.OrderRef) < 6)
        sprintf(req.OrderRef, "%06d", atoi(req.OrderRef));
    if ((strcmp(req.ExchangeID, "ctp") == 0 || strcmp(req.ExchangeID, "CTP") == 0) && markets.size() != 0)
        memcpy(req.ExchangeID, markets[0].c_str(), 9);
    //WhiteList
    std::string ticker = account_units[account_index].coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if (ticker.length() == 0)
        KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList, send it as original value (rid)" << requestId << " (InstrumentID)" << data->InstrumentID);
    else
        memcpy(req.InstrumentID, ticker.c_str(), sizeof(req.InstrumentID));
    //scale_offset
    req.LimitPrice = data->LimitPrice * 1.0 / scale_offset;//double
    req.VolumeTotalOriginal = data->Volume / scale_offset; //int
    req.MinVolume = data->MinVolume / scale_offset;        //int
    req.StopPrice = data->StopPrice * 1.0 / scale_offset;  //double
    //

    KF_LOG_DEBUG(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (BrokerID)" << req.BrokerID
                                              << " (Iid)" << req.InvestorID
                                              << " (ExchangeID)" << req.ExchangeID
                                              << " (UserID)" << req.UserID
                                              << " (Tid)" << req.InstrumentID
                                              << " (OrderRef)" << req.OrderRef
                                              << " (account_index)" << account_index);
    if(req.TimeCondition == THOST_FTDC_TC_GTC)
        req.TimeCondition = THOST_FTDC_TC_GFD;

    //KF_LOG_DEBUG(logger, "[req_order_insert] OrderPriceType " << req.OrderPriceType << " Direction " << req.Direction <<" LimitPrice " << req.LimitPrice <<" VolumeTotalOriginal "<< req.VolumeTotalOriginal);
    //KF_LOG_DEBUG(logger, "[req_order_insert] CombOffsetFlag " << req.CombOffsetFlag[0] << " CombHedgeFlag " << req.CombHedgeFlag[0]);
    //KF_LOG_DEBUG(logger, "[req_order_insert] ContingentCondition " << req.ContingentCondition);
    //KF_LOG_DEBUG(logger, "[req_order_insert] TimeCondition "  << req.TimeCondition << " VolumeCondition " << req.VolumeCondition << " MinVolume " << req.MinVolume);
    //KF_LOG_DEBUG(logger, "[req_order_insert] ForceCloseReason " << req.ForceCloseReason);
    int errorId = 0;
    std::string errorMsg = "";
    on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
    if (account_units[account_index].api->ReqOrderInsert(&req, requestId))
    {
        KF_LOG_ERROR(logger, "[request] order insert failed!" << " (rid)" << requestId);
    }
    send_writer->write_frame(&req, sizeof(CThostFtdcInputOrderField), source_id, MSG_TYPE_LF_ORDER_CTP, 1/*ISLAST*/, requestId);
}

void TDEngineCTP::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    struct CThostFtdcInputOrderActionField req = parseTo(*data);
    req.OrderActionRef = local_id ++;
    auto& unit = account_units[account_index];
    req.FrontID = unit.front_id;
    req.SessionID = unit.session_id;
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (Iid)" << req.InvestorID
                                              << " (OrderRef)" << req.OrderRef
                                              << " (OrderActionRef)" << req.OrderActionRef);

    if (unit.api->ReqOrderAction(&req, requestId))
    {
        KF_LOG_ERROR(logger, "[request] order action failed!" << " (rid)" << requestId);
    }

    send_writer->write_frame(&req, sizeof(CThostFtdcInputOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_CTP, 1/*ISLAST*/, requestId);
}

/*
 * SPI functions
 */
void TDEngineCTP::OnFrontConnected()
{
    KF_LOG_INFO(logger, "[OnFrontConnected] (idx)" << curAccountIdx);
    account_units[curAccountIdx].connected = true;
}

void TDEngineCTP::OnFrontDisconnected(int nReason)
{
    KF_LOG_INFO(logger, "[OnFrontDisconnected] reason=" << nReason);
    for (auto& unit: account_units)
    {
        unit.connected = false;
        unit.authenticated = false;
        unit.settle_confirmed = false;
        unit.logged_in = false;
    }
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

void TDEngineCTP::OnRspAuthenticate(CThostFtdcRspAuthenticateField *pRspAuthenticateField,
                                    CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInfo != nullptr && pRspInfo->ErrorID != 0)
    {
        KF_LOG_ERROR(logger, "[OnRspAuthenticate]" << " (errId)" << pRspInfo->ErrorID
                                                   << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg));
    }
    else
    {
        KF_LOG_INFO(logger, "[OnRspAuthenticate]" << " (userId)" <<  pRspAuthenticateField->UserID
                                                  << " (brokerId)" << pRspAuthenticateField->BrokerID
                                                  << " (product)" << pRspAuthenticateField->UserProductInfo
                                                  << " (rid)" << nRequestID);
        for (auto& unit: account_units)
        {
            if (unit.auth_rid == nRequestID)
                unit.authenticated = true;
        }
    }
}

void TDEngineCTP::OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo,
                                 int nRequestID, bool bIsLast)
{
    if (pRspInfo != nullptr && pRspInfo->ErrorID != 0)
    {
        KF_LOG_ERROR(logger, "[OnRspUserLogin]" << " (errId)" << pRspInfo->ErrorID
                                                << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg));
    }
    else
    {
        KF_LOG_INFO(logger, "[OnRspUserLogin]" << " (Bid)" << pRspUserLogin->BrokerID
                                               << " (Uid)" << pRspUserLogin->UserID
                                               << " (maxRef)" << pRspUserLogin->MaxOrderRef
                                               << " (Fid)" << pRspUserLogin->FrontID
                                               << " (Sid)" << pRspUserLogin->SessionID);
        for (auto& unit: account_units)
        {
            if (unit.login_rid == nRequestID)
            {
                unit.front_id = pRspUserLogin->FrontID;
                unit.session_id = pRspUserLogin->SessionID;
                unit.logged_in = true;
            }
        }
        int max_ref = atoi(pRspUserLogin->MaxOrderRef) + 1;
        local_id = (max_ref > local_id) ? max_ref: local_id;
    }
}

void TDEngineCTP::OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField *pSettlementInfoConfirm,
                                             CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInfo != nullptr && pRspInfo->ErrorID != 0)
    {
        KF_LOG_ERROR(logger, "[OnRspSettlementInfoConfirm]" << " (errId)" << pRspInfo->ErrorID
                                                            << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg));
    }
    else
    {
        KF_LOG_INFO(logger, "[OnRspSettlementInfoConfirm]" << " (brokerID)" << pSettlementInfoConfirm->BrokerID
                                                           << " (investorID)" << pSettlementInfoConfirm->InvestorID
                                                           << " (confirmDate)" << pSettlementInfoConfirm->ConfirmDate
                                                           << " (confirmTime)" << pSettlementInfoConfirm->ConfirmTime);
        for (auto& unit: account_units)
        {
            if (unit.settle_rid == nRequestID)
            {
                unit.settle_confirmed = true;
            }
        }
    }
}

void TDEngineCTP::OnRspUserLogout(CThostFtdcUserLogoutField *pUserLogout, CThostFtdcRspInfoField *pRspInfo,
                                  int nRequestID, bool bIsLast)
{
    if (pRspInfo != nullptr && pRspInfo->ErrorID == 0)
    {
        KF_LOG_ERROR(logger, "[OnRspUserLogout]" << " (errId)" << pRspInfo->ErrorID
                                                 << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg));
    }
    else
    {
        KF_LOG_INFO(logger, "[OnRspUserLogout]" << " (brokerId)" << pUserLogout->BrokerID
                                                << " (userId)" << pUserLogout->UserID);
        for (auto& unit: account_units)
        {
            if (unit.login_rid == nRequestID)
            {
                unit.logged_in = false;
                unit.authenticated = false;
                unit.settle_confirmed = false;
            }
        }
    }
}

void TDEngineCTP::OnRspOrderInsert(CThostFtdcInputOrderField *pInputOrder, CThostFtdcRspInfoField *pRspInfo,
                                    int nRequestID, bool bIsLast)
{
    int errorId = (pRspInfo == nullptr) ? 0 : pRspInfo->ErrorID;
    const char* errorMsg = (pRspInfo == nullptr) ? nullptr : EngineUtil::gbkErrorMsg2utf8(pRspInfo->ErrorMsg);
    auto data = parseFrom(*pInputOrder);
    //scale_offset
    data.LimitPrice = pInputOrder->LimitPrice * scale_offset;
    data.Volume = pInputOrder->VolumeTotalOriginal * scale_offset;
    data.MinVolume = pInputOrder->MinVolume * scale_offset;
    data.StopPrice = pInputOrder->StopPrice * scale_offset;
    //
    on_rsp_order_insert(&data, nRequestID, errorId, errorMsg);
    KF_LOG_INFO(logger, "[OnRspOrderInsert]" << " (errorMsg) " << errorMsg << " (nRequestID) " << nRequestID);
    raw_writer->write_error_frame(pInputOrder, sizeof(CThostFtdcInputOrderField), source_id, MSG_TYPE_LF_ORDER_CTP, bIsLast, nRequestID, errorId, errorMsg);
}

void TDEngineCTP::OnRspOrderAction(CThostFtdcInputOrderActionField *pInputOrderAction,
                                   CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    int errorId = (pRspInfo == nullptr) ? 0 : pRspInfo->ErrorID;
    const char* errorMsg = (pRspInfo == nullptr) ? nullptr : EngineUtil::gbkErrorMsg2utf8(pRspInfo->ErrorMsg);
    auto data = parseFrom(*pInputOrderAction);
    on_rsp_order_action(&data, nRequestID, errorId, errorMsg);
    KF_LOG_INFO(logger, "[OnRspOrderAction]" << " (errorMsg) " << errorMsg << " (nRequestID) " << nRequestID);
    raw_writer->write_error_frame(pInputOrderAction, sizeof(CThostFtdcInputOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_CTP, bIsLast, nRequestID, errorId, errorMsg);
}

void TDEngineCTP::OnRspQryInvestorPosition(CThostFtdcInvestorPositionField *pInvestorPosition, CThostFtdcRspInfoField *pRspInfo,
                                     int nRequestID, bool bIsLast)
{
    int errorId = (pRspInfo == nullptr) ? 0 : pRspInfo->ErrorID;
    const char* errorMsg = (pRspInfo == nullptr) ? nullptr : EngineUtil::gbkErrorMsg2utf8(pRspInfo->ErrorMsg);
    CThostFtdcInvestorPositionField emptyCtp = {};
    if (pInvestorPosition == nullptr)
        pInvestorPosition = &emptyCtp;
    auto pos = parseFrom(*pInvestorPosition);
    on_rsp_position(&pos, bIsLast, nRequestID, errorId, errorMsg);
    raw_writer->write_error_frame(pInvestorPosition, sizeof(CThostFtdcInvestorPositionField), source_id, MSG_TYPE_LF_RSP_POS_CTP, bIsLast, nRequestID, errorId, errorMsg);
}

void TDEngineCTP::OnRtnOrder(CThostFtdcOrderField *pOrder)
{
    KF_LOG_INFO(logger, "[OnRtnOrder]" << " (OrderType) " << pOrder->OrderType
                                       << " (OrderSubmitStatus) " << pOrder->OrderSubmitStatus
                                       << " (OrderStatus) " << pOrder->OrderStatus);
    auto rtn_order = parseFrom(*pOrder);
    //scale_offset
    rtn_order.VolumeTraded = pOrder->VolumeTraded * scale_offset;
    rtn_order.VolumeTotalOriginal = pOrder->VolumeTotalOriginal * scale_offset;
    rtn_order.VolumeTotal = pOrder->VolumeTotal * scale_offset;
    rtn_order.LimitPrice = pOrder->LimitPrice * scale_offset;
    //
    on_rtn_order(&rtn_order);
    raw_writer->write_frame(pOrder, sizeof(CThostFtdcOrderField),
                            source_id, MSG_TYPE_LF_RTN_ORDER_CTP,
                            1/*islast*/, (pOrder->RequestID > 0) ? pOrder->RequestID: -1);
}

void TDEngineCTP::OnRtnTrade(CThostFtdcTradeField *pTrade)
{
    KF_LOG_INFO(logger, "[OnRtnTrade]" << " (Volume) " << pTrade->Volume);
    auto rtn_trade = parseFrom(*pTrade);
    //scale_offset
    rtn_trade.Price = pTrade->Price * scale_offset;
    rtn_trade.Volume = pTrade->Volume * scale_offset;
    //
    on_rtn_trade(&rtn_trade);
    raw_writer->write_frame(pTrade, sizeof(CThostFtdcTradeField),
                            source_id, MSG_TYPE_LF_RTN_TRADE_CTP, 1/*islast*/, -1/*invalidRid*/);
}

void TDEngineCTP::OnRspQryTradingAccount(CThostFtdcTradingAccountField *pTradingAccount,
                                         CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    int errorId = (pRspInfo == nullptr) ? 0 : pRspInfo->ErrorID;
    const char* errorMsg = (pRspInfo == nullptr) ? nullptr : EngineUtil::gbkErrorMsg2utf8(pRspInfo->ErrorMsg);
    CThostFtdcTradingAccountField empty = {};
    if (pTradingAccount == nullptr)
        pTradingAccount = &empty;
    auto account = parseFrom(*pTradingAccount);
    on_rsp_account(&account, bIsLast, nRequestID, errorId, errorMsg);
    raw_writer->write_error_frame(pTradingAccount, sizeof(CThostFtdcTradingAccountField), source_id, MSG_TYPE_LF_RSP_ACCOUNT_CTP, bIsLast, nRequestID, errorId, errorMsg);
}

BOOST_PYTHON_MODULE(libctptd)
{
    using namespace boost::python;
    class_<TDEngineCTP, boost::shared_ptr<TDEngineCTP> >("Engine")
    .def(init<>())
    .def("init", &TDEngineCTP::initialize)
    .def("start", &TDEngineCTP::start)
    .def("stop", &TDEngineCTP::stop)
    .def("logout", &TDEngineCTP::logout)
    .def("wait_for_stop", &TDEngineCTP::wait_for_stop);
}