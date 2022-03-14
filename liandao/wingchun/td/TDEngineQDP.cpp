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

#include "TDEngineQDP.h"
#include "longfist/qdp.h"
#include "longfist/LFUtils.h"
#include "TypeConvert.hpp"
#include <stdlib.h>
#include <cstdlib>  
#include <string.h>
#include <boost/algorithm/string.hpp>
#include <chrono>

USING_WC_NAMESPACE

TDEngineQDP::TDEngineQDP(): ITDEngine(SOURCE_QDP), need_authenticate(false), curAccountIdx(-1)
//TDEngineQDP::TDEngineQDP(): ITDEngine(SOURCE_QDP),need_settleConfirm(true), need_authenticate(false), curAccountIdx(-1)
{
    logger = yijinjing::KfLog::getLogger("TradeEngine.QDP");
    KF_LOG_INFO(logger, "[ATTENTION] default to confirm settlement and no authentication!");
}

void TDEngineQDP::init()
{
    ITDEngine::init();
    JournalPair tdRawPair = getTdRawJournalPair(source_id);
    raw_writer = yijinjing::JournalWriter::create(tdRawPair.first, tdRawPair.second, "RAW_" + name());
}

void TDEngineQDP::pre_load(const json& j_config)
{
    KF_LOG_INFO(logger, "TDEngineQDP::pre_load");
    front_uri = j_config["WC_CONFIG_KEY_FRONT_URI"].get<string>();
    /*if (j_config.find("WC_CONFIG_KEY_NEED_SETTLE_CONFIRM") != j_config.end()
        && !j_config["WC_CONFIG_KEY_NEED_SETTLE_CONFIRM"].get<bool>())
    {
        need_settleConfirm = false;
        KF_LOG_INFO(logger, "[ATTENTION] no need to confirm settlement!");
    }
    */
    if (j_config.find("WC_CONFIG_KEY_NEED_AUTH") != j_config.end()
        && j_config["WC_CONFIG_KEY_NEED_AUTH"].get<bool>())
    {
        need_authenticate = true;
        KF_LOG_INFO(logger, "[ATTENTION] need to authenticate code!");
    }
}

void TDEngineQDP::resize_accounts(int account_num)
{
    account_units.resize(account_num);
}

TradeAccount TDEngineQDP::load_account(int idx, const json& j_config)
{
    KF_LOG_INFO(logger, "TDEngineQDP::load_account");
    // internal load

    //from demo : strncpy(qryInvestor.InvestorID, g_config.UserID, sizeof(qryInvestor.InvestorID));
    string broker_id = j_config["WC_CONFIG_KEY_BROKER_ID"].get<string>();
    string user_id = j_config["WC_CONFIG_KEY_USER_ID"].get<string>();
    string investor_id = j_config["WC_CONFIG_KEY_INVESTOR_ID"].get<string>();
    string password = j_config["WC_CONFIG_KEY_PASSWORD"].get<string>();

    AccountUnitQDP& unit = account_units[idx];
    unit.api = nullptr;
    unit.front_id = -1;
    unit.session_id = -1;
    unit.initialized = false;
    unit.connected = false;
    unit.authenticated = false;
    unit.logged_in = false;
    //unit.settle_confirmed = false;
    if (need_authenticate) {
        unit.auth_code = j_config["WC_CONFIG_KEY_AUTH_CODE"].get<string>();
        unit.app_id = j_config["WC_CONFIG_KEY_APP_ID"].get<string>();
        unit.user_product_info = j_config["WC_CONFIG_KEY_USER_PRODUCT_INFO"].get<string>();
    }

    if (j_config.find("markets") != j_config.end()) {
        KF_LOG_INFO(logger, "TDEngineQDP::load markets");
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
    KF_LOG_INFO(logger, "TDEngineQDP::load_account try ro connect");
    connect(5000000000);
    //end
    return account;
}

void TDEngineQDP::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "TDEngineQDP::connect");
    for (int idx = 0; idx < account_units.size(); idx ++)
    {
        AccountUnitQDP& unit = account_units[idx];
        if (unit.api == nullptr)
        {
            CQdpFtdcTraderApi* api = CQdpFtdcTraderApi::CreateFtdcTraderApi();
            if (!api)
            {
                throw std::runtime_error("QDP_TD failed to create api");
            }
            api->RegisterSpi(this);
            unit.api = api;
        }
        if (!unit.connected)
        {
            curAccountIdx = idx;
            unit.api->SubscribePublicTopic(QDP_TERT_QUICK); // need check
            unit.api->SubscribePrivateTopic(QDP_TERT_QUICK); // need check
            unit.api->RegisterFront((char*)front_uri.c_str());
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
    KF_LOG_INFO(logger, "TDEngineQDP::connect try to login");
    login(5000000000);
    //end
}

void TDEngineQDP::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "TDEngineQDP::login");
    for (int idx = 0; idx < account_units.size(); idx ++)
    {
        KF_LOG_INFO(logger, "TDEngineQDP::login idx" << idx);
        AccountUnitQDP& unit = account_units[idx];
        TradeAccount& account = accounts[idx];
        // authenticate
        if (need_authenticate && !unit.authenticated)
        {
            KF_LOG_INFO(logger, "TDEngineQDP::login, prepare to authenticate");
            struct CQdpFtdcAuthenticateField req = {0};
            strcpy(req.BrokerID, account.BrokerID);
            strcpy(req.UserID, account.UserID);
            strcpy(req.UserProductInfo, unit.user_product_info.c_str());
            strcpy(req.AppID, unit.app_id.c_str());
            strcpy(req.AuthCode, unit.auth_code.c_str());
            unit.auth_rid = request_id;
            //log for ReqAuthenticate
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.BrokerID " << req.BrokerID);
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.UserID " << req.UserID);
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.UserProductInfo " << req.UserProductInfo);
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.AppID " << req.AppID);
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.AuthCode " << req.AuthCode);
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
                KF_LOG_INFO(logger, "TDEngineQDP::login, ReqAuthenticate iResult: " << iResult);
            long start_time = yijinjing::getNanoTime();
            while (!unit.authenticated && yijinjing::getNanoTime() - start_time < timeout_nsec) {}
        }
        // login
        if (!unit.logged_in)
        {
            KF_LOG_INFO(logger, "TDEngineQDP::login, prepare to login");
            struct CQdpFtdcReqUserLoginField req = {0};
            strcpy(req.TradingDay, "");
            strcpy(req.UserID, account.UserID);
            strcpy(req.BrokerID, account.BrokerID);
            strcpy(req.Password, account.Password);
            unit.login_rid = request_id;
            //log for ReqUserLogin
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.UserID: " << req.UserID);
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.BrokerID: " << req.BrokerID);
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.Password: " << req.Password);
            KF_LOG_INFO(logger, "TDEngineQDP::login, request_id " << request_id);
            //if (unit.api->ReqUserLogin(&req, request_id++))
            int iResult = unit.api->ReqUserLogin(&req, request_id++);
            if (iResult != 0){
                KF_LOG_ERROR(logger, "[request] login failed!" << " (Bid)" << req.BrokerID
                                                               << " (Uid)" << req.UserID
                                                               << " (iResult)" << iResult);
                /*
                1、必须使用root用户
                2、必须安装lshw模块
                */
            }
            else
                KF_LOG_INFO(logger, "TDEngineQDP::login, ReqUserLogin iResult: " << iResult);
            long start_time = yijinjing::getNanoTime();
            while (!unit.logged_in && yijinjing::getNanoTime() - start_time < timeout_nsec) {}
        }
        if (unit.logged_in)
        {
            CQdpFtdcQryUserInvestorField reqField = { 0 };
            strncpy(reqField.BrokerID, account.BrokerID, sizeof(reqField.BrokerID));
            strncpy(reqField.UserID, account.UserID, sizeof(reqField.UserID));
            int iResult = unit.api->ReqQryUserInvestor(&reqField, request_id++);
            if (iResult != 0) {
                KF_LOG_ERROR(logger, "[request] ReqQryUserInvestor failed!" << " (Bid)" << reqField.BrokerID
                    << " (Uid)" << reqField.UserID << " (iResult)" << iResult);
            }
            else
                KF_LOG_INFO(logger, "TDEngineQDP::login, ReqQryUserInvestor iResult: " << iResult);
        }
        // confirm settlement
        /*
        if (need_settleConfirm && !unit.settle_confirmed)
        {
            KF_LOG_INFO(logger, "TDEngineQDP::login, need_settleConfirm && !unit.settle_confirmed");
            struct CThostFtdcSettlementInfoConfirmField req = {0};
            strcpy(req.BrokerID, account.BrokerID);
            strcpy(req.InvestorID, account.InvestorID);
            unit.settle_rid = request_id;
            //log for ReqSettlementInfoConfirm
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.BrokerID: " << req.BrokerID);
            KF_LOG_INFO(logger, "TDEngineQDP::login, req.InvestorID: " << req.InvestorID);
            KF_LOG_INFO(logger, "TDEngineQDP::login, request_id " << request_id);
            //if (unit.api->ReqSettlementInfoConfirm(&req, request_id++))
            int iResult = unit.api->ReqSettlementInfoConfirm(&req, request_id++);
            if (iResult != 0)
                KF_LOG_ERROR(logger, "[request] settlement info failed!" << " (Bid)" << req.BrokerID
                                                                         << " (Iid)" << req.InvestorID
                                                                         << " (iResult)" << iResult);
            else
                KF_LOG_INFO(logger, "TDEngineQDP::login, ReqSettlementInfoConfirm iResult: " << iResult);
            long start_time = yijinjing::getNanoTime();
            while (!unit.settle_confirmed && yijinjing::getNanoTime() - start_time < timeout_nsec)
            {}
            KF_LOG_INFO(logger, "TDEngineQDP::login," << " unit.settle_confirmed: " << unit.settle_confirmed);
        }
        */
    }
}

void TDEngineQDP::logout()
{
    KF_LOG_INFO(logger, "TDEngineQDP::logout");
    for (int idx = 0; idx < account_units.size(); idx++)
    {
        AccountUnitQDP& unit = account_units[idx];
        TradeAccount& account = accounts[idx];
        if (unit.logged_in)
        {
            CQdpFtdcReqUserLogoutField req = {};
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
        //unit.settle_confirmed = false;
        unit.logged_in = false;
    }
}

void TDEngineQDP::release_api()
{
    KF_LOG_INFO(logger, "TDEngineQDP::release_api");
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
        //unit.settle_confirmed = false;
        unit.logged_in = false;
        unit.api = nullptr;
    }
}

bool TDEngineQDP::is_logged_in() const
{
    for (auto& unit: account_units)
    {
        //if (!unit.logged_in || (need_settleConfirm && !unit.settle_confirmed))
        if (!unit.logged_in)
            return false;
    }
    return true;
}

bool TDEngineQDP::is_connected() const
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
void TDEngineQDP::req_investor_position(const LFQryPositionField* data, int account_index, int requestId)
{
    struct CQdpFtdcQryInvestorPositionField req = parseTo(*data);
    KF_LOG_DEBUG(logger, "[req_pos]" << " (Bid)" << req.BrokerID
                                     << " (Iid)" << req.InvestorID
                                     << " (Tid)" << req.InstrumentID);

    if (account_units[account_index].api->ReqQryInvestorPosition(&req, requestId))
    {
        KF_LOG_ERROR(logger, "[request] investor position failed!" << " (rid)" << requestId
                                                                   << " (idx)" << account_index);
    }
    send_writer->write_frame(&req, sizeof(CQdpFtdcQryInvestorPositionField), source_id, MSG_TYPE_LF_QRY_POS_QDP, 1/*ISLAST*/, requestId);
}

void TDEngineQDP::req_qry_account(const LFQryAccountField *data, int account_index, int requestId)
{
    struct CQdpFtdcQryInvestorAccountField req = parseTo(*data);
    KF_LOG_DEBUG(logger, "[req_account]" << " (Bid)" << req.BrokerID
                                         << " (Iid)" << req.InvestorID);
    //ReqQryInvestorAccount(CQdpFtdcQryInvestorAccountField *pQryInvestorAccount, int nRequestID)
    //if (account_units[account_index].api->ReqQryTradingAccount(&req, requestId))
    if (account_units[account_index].api->ReqQryInvestorAccount(&req, requestId))
    {
        KF_LOG_ERROR(logger, "[request] account info failed!" << " (rid)" << requestId
                                                              << " (idx)" << account_index);
    }
    //potential problem
    send_writer->write_frame(&req, sizeof(CQdpFtdcQryInvestorAccountField), source_id, MSG_TYPE_LF_QRY_ACCOUNT_QDP, 1/*ISLAST*/, requestId);
}

void TDEngineQDP::req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time)
{
    struct CQdpFtdcInputOrderField req = parseTo(*data);
    req.IsAutoSuspend = 0;
    req.ForceCloseReason = QDP_FTDC_FCR_NotForceClose;
    if ((strcmp(req.ExchangeID, "qdp") == 0 || strcmp(req.ExchangeID, "QDP") == 0) && markets.size() != 0)
        memcpy(req.ExchangeID, markets[0].c_str(), 9);
    if (req.TimeCondition == QDP_FTDC_TC_GTC)
        req.TimeCondition = QDP_FTDC_TC_GFD;
    //WhiteList
    std::string ticker = account_units[account_index].coinPairWhiteList.GetValueByKey(std::string(data->InstrumentID));
    if (ticker.length() == 0)
        KF_LOG_ERROR(logger, "[req_order_insert]: not in WhiteList, send it as original value (rid)" << requestId << " (InstrumentID)" << data->InstrumentID);
    else
        memcpy(req.InstrumentID, ticker.c_str(), sizeof(req.InstrumentID));
    //scale_offset
    req.LimitPrice = data->LimitPrice * 1.0 / scale_offset;//double
    req.Volume = data->Volume / scale_offset;              //int
    req.MinVolume = data->MinVolume / scale_offset;        //int
    req.StopPrice = data->StopPrice * 1.0 / scale_offset;  //double
    //
    KF_LOG_INFO(logger, "[req_order_insert]" << " (rid)" << requestId
                                              << " (BrokerID)" << req.BrokerID
                                              << " (Iid)" << req.InvestorID
                                              << " (ExchangeID)" << req.ExchangeID
                                              << " (UserID)" << req.UserID
                                              << " (Tid)" << req.InstrumentID
                                              << " (UserOrderLocalID)" << req.UserOrderLocalID
                                              << " (InvestorID)" << req.InvestorID
                                              << " (account_index)" << account_index);
    KF_LOG_INFO(logger, "[req_order_insert]"  << " (LimitPrice)" << req.LimitPrice 
                                              << " (StopPrice)" << req.StopPrice
                                              << " (Volume)" << req.Volume
                                              << " (MinVolume)" << req.MinVolume
                                              << " (TimeCondition)" << req.TimeCondition
                                              << " (OrderPriceType)" << req.OrderPriceType);
    int errorId = 0;
    std::string errorMsg = "";
    on_rsp_order_insert(data, requestId, errorId, errorMsg.c_str());
    if (account_units[account_index].api->ReqOrderInsert(&req, requestId))
    {
        KF_LOG_ERROR(logger, "[request] order insert failed!" << " (rid)" << requestId);
    }
    send_writer->write_frame(&req, sizeof(CQdpFtdcInputOrderField), source_id, MSG_TYPE_LF_ORDER_QDP, 1/*ISLAST*/, requestId);
}

void TDEngineQDP::req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time)
{
    struct CQdpFtdcOrderActionField req = parseTo(*data);
    //map for OrderRef/UserOrderLocalID and OrderSysID
    string UID(data->OrderRef);
    auto itr = UID_SID.find(UID);
    if (itr != UID_SID.end()) {
        string SID = itr->second;
        KF_LOG_DEBUG(logger, "erase from map," << " (SID)" << SID << " (UID)" << UID);
        strncpy(req.OrderSysID, SID.c_str(), sizeof(req.OrderSysID));
        UID_SID.erase(itr);
    }
    else
        KF_LOG_INFO(logger, "can not found OrderSysID with the UserOrderLocalID");
    //req.OrderActionRef = local_id ++;
    req.RecNum = local_id ++;
    auto& unit = account_units[account_index];
    req.FrontID = unit.front_id;
    req.SessionID = unit.session_id;
    KF_LOG_DEBUG(logger, "[req_order_action]" << " (rid)" << requestId
                                              << " (Iid)" << req.InvestorID
                                              << " (UserOrderActionLocalID)" << req.UserOrderActionLocalID);

    string exchangeID = req.ExchangeID;
    if ((exchangeID.length() == 0 || exchangeID == "QDP" || exchangeID == "qdp") && markets.size() != 0) {
        KF_LOG_ERROR(logger, "[req_order_action] cancel order without a corret exchangeID" << " (rid)" << requestId);
        KF_LOG_ERROR(logger, "[req_order_action] use exchangeID in \"markets\" of kungfu.json ");

        strncpy(req.ExchangeID, markets[0].c_str(), sizeof(req.ExchangeID));
        if (unit.api->ReqOrderAction(&req, requestId))
            KF_LOG_ERROR(logger, "[request] order action failed!" << " (rid)" << requestId);
    }

    else if (unit.api->ReqOrderAction(&req, requestId))
        KF_LOG_ERROR(logger, "[request] order action failed!" << " (rid)" << requestId);

    send_writer->write_frame(&req, sizeof(CQdpFtdcOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_QDP, 1/*ISLAST*/, requestId);
}

/*
 * SPI functions
 */
void TDEngineQDP::OnFrontConnected()
{
    KF_LOG_INFO(logger, "[OnFrontConnected] (idx)" << curAccountIdx);
    account_units[curAccountIdx].connected = true;
}

void TDEngineQDP::OnFrontDisconnected(int nReason)
{
    KF_LOG_INFO(logger, "[OnFrontDisconnected] reason=" << nReason);
    for (auto& unit: account_units)
    {
        unit.connected = false;
        unit.authenticated = false;
        //unit.settle_confirmed = false;
        unit.logged_in = false;
    }
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

void TDEngineQDP::OnRspAuthenticate(CQdpFtdcRtnAuthenticateField* pRtnAuthenticate, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInfo != nullptr && pRspInfo->ErrorID != 0)
    {
        KF_LOG_ERROR(logger, "[OnRspAuthenticate]" << " (errId)" << pRspInfo->ErrorID
                                                   << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg));
    }
    else
    {
        KF_LOG_INFO(logger, "[OnRspAuthenticate]" << " (userId)" << pRtnAuthenticate->UserID
                                                  << " (brokerId)" << pRtnAuthenticate->BrokerID
                                                  << " (product)" << pRtnAuthenticate->UserProductInfo
                                                  << " (rid)" << nRequestID);
        for (auto& unit: account_units)
        {
            if (unit.auth_rid == nRequestID)
                unit.authenticated = true;
        }
    }
}

void TDEngineQDP::OnRspUserLogin(CQdpFtdcRspUserLoginField* pRspUserLogin, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast)
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
                                               //<< " (maxRef)" << pRspUserLogin->MaxOrderRef
                                               << " (MaxOrderLocalID)" << pRspUserLogin->MaxOrderLocalID
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
        //int max_ref = atoi(pRspUserLogin->MaxOrderRef) + 1;
        int max_ref = atoi(pRspUserLogin->MaxOrderLocalID) + 1;
        local_id = (max_ref > local_id) ? max_ref: local_id;
    }
}

void TDEngineQDP::OnRspQryUserInvestor(CQdpFtdcRspUserInvestorField* pRspUserInvestor, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInfo && pRspInfo->ErrorID != 0)
    {
        KF_LOG_ERROR(logger, "[OnRspQryUserInvestor]" << " (errId)" << pRspInfo->ErrorID
            << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg));
    }
    else
    {
        KF_LOG_INFO(logger, "[OnRspQryUserInvestor]" << " (brokerId)" << pRspUserInvestor->BrokerID
                                                     << " (userId)" << pRspUserInvestor->UserID
                                                     << " (InvestorID)" << pRspUserInvestor->InvestorID);
        for (int idx = 0; idx < account_units.size(); idx++)
        {
            if (strcmp(accounts[idx].UserID, pRspUserInvestor->UserID) == 0) {
                strncpy(accounts[idx].InvestorID, pRspUserInvestor->InvestorID, sizeof(accounts[idx].InvestorID));
                KF_LOG_INFO(logger, "[OnRspQryUserInvestor] set InvestorID successed");
            }
        }
        //请求查询合约
        //CQdpFtdcQryInstrumentField reqField = { 0 };
        //int iResult = m_pApi->ReqQryInstrument(&reqField, m_requestID++);
    }
}

void TDEngineQDP::OnRspUserLogout(CQdpFtdcRspUserLogoutField* pRspUserLogout, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInfo != nullptr && pRspInfo->ErrorID == 0)
    {
        KF_LOG_ERROR(logger, "[OnRspUserLogout]" << " (errId)" << pRspInfo->ErrorID
                                                 << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg));
    }
    else
    {
        KF_LOG_INFO(logger, "[OnRspUserLogout]" << " (brokerId)" << pRspUserLogout->BrokerID
                                                << " (userId)" << pRspUserLogout->UserID);
        for (auto& unit: account_units)
        {
            if (unit.login_rid == nRequestID)
            {
                unit.logged_in = false;
                unit.authenticated = false;
                //unit.settle_confirmed = false;
            }
        }
    }
}

void TDEngineQDP::OnRspOrderInsert(CQdpFtdcInputOrderField* pInputOrder, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast)
{
    int errorId = (pRspInfo == nullptr) ? 0 : pRspInfo->ErrorID;
    const char* errorMsg = (pRspInfo == nullptr) ? nullptr : EngineUtil::gbkErrorMsg2utf8(pRspInfo->ErrorMsg);
    LFInputOrderField data = parseFrom(*pInputOrder);
    //scale_offset
    data.LimitPrice = pInputOrder->LimitPrice * scale_offset;
    data.Volume = pInputOrder->Volume * scale_offset;
    data.MinVolume = pInputOrder->MinVolume * scale_offset;
    data.StopPrice = pInputOrder->StopPrice * scale_offset;
    //
    //on_rsp_order_insert(&data, nRequestID, errorId, errorMsg);
    KF_LOG_INFO(logger, "[OnRspOrderInsert]" << " (errorMsg) " << errorMsg << " (nRequestID) " << nRequestID);
    raw_writer->write_error_frame(pInputOrder, sizeof(CQdpFtdcInputOrderField), source_id, MSG_TYPE_LF_ORDER_QDP, bIsLast, nRequestID, errorId, errorMsg);
}

void TDEngineQDP::OnRspOrderAction(CQdpFtdcOrderActionField* pOrderAction, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast)
{
    int errorId = (pRspInfo == nullptr) ? 0 : pRspInfo->ErrorID;
    const char* errorMsg = (pRspInfo == nullptr) ? nullptr : EngineUtil::gbkErrorMsg2utf8(pRspInfo->ErrorMsg);
    LFOrderActionField data = parseFrom(*pOrderAction);
    on_rsp_order_action(&data, nRequestID, errorId, errorMsg);
    KF_LOG_INFO(logger, "[OnRspOrderAction]" << " (errorMsg) " << errorMsg << " (nRequestID) " << nRequestID);
    raw_writer->write_error_frame(pOrderAction, sizeof(CQdpFtdcOrderActionField), source_id, MSG_TYPE_LF_ORDER_ACTION_QDP, bIsLast, nRequestID, errorId, errorMsg);
}

void TDEngineQDP::OnRspQryInvestorPosition(CQdpFtdcRspInvestorPositionField* pRspInvestorPosition, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast)
{
    int errorId = (pRspInfo == nullptr) ? 0 : pRspInfo->ErrorID;
    const char* errorMsg = (pRspInfo == nullptr) ? nullptr : EngineUtil::gbkErrorMsg2utf8(pRspInfo->ErrorMsg);
    CQdpFtdcRspInvestorPositionField emptyQdp = {};
    if (pRspInvestorPosition == nullptr)
        pRspInvestorPosition = &emptyQdp;
    LFRspPositionField pos = parseFrom(*pRspInvestorPosition);
    on_rsp_position(&pos, bIsLast, nRequestID, errorId, errorMsg);
    raw_writer->write_error_frame(pRspInvestorPosition, sizeof(CQdpFtdcRspInvestorPositionField), source_id, MSG_TYPE_LF_RSP_POS_QDP, bIsLast, nRequestID, errorId, errorMsg);
}

void TDEngineQDP::OnRtnOrder(CQdpFtdcOrderField* pOrder)
{
    KF_LOG_INFO(logger, "[OnRtnOrder]" //<< " (OrderType) " << pOrder->OrderType
                                       //<< " (OrderSubmitStatus) " << pOrder->OrderSubmitStatus
                                       << " (OrderStatus) " << pOrder->OrderStatus);
    LFRtnOrderField rtn_order = parseFrom(*pOrder);
    //scale_offset
    rtn_order.VolumeTraded = pOrder->VolumeTraded * scale_offset;
    rtn_order.VolumeTotalOriginal = pOrder->Volume * scale_offset;
    rtn_order.VolumeTotal = pOrder->VolumeRemain * scale_offset;
    rtn_order.LimitPrice = pOrder->LimitPrice * scale_offset;
    //map for OrderRef and OrderSysID
    if (pOrder->OrderStatus != QDP_FTDC_OS_AllTraded && pOrder->OrderStatus != QDP_FTDC_OS_NoTradeNotQueueing) {
        string UID(pOrder->UserOrderLocalID);
        string SID(pOrder->OrderSysID);
        UID_SID.insert(make_pair(UID, SID));
    }
    //erase from map
    if (pOrder->OrderStatus == QDP_FTDC_OS_AllTraded || pOrder->OrderStatus == QDP_FTDC_OS_NoTradeNotQueueing) {
        string UID(pOrder->UserOrderLocalID);
        auto itr = UID_SID.find(UID);
        if (itr != UID_SID.end()) {
            KF_LOG_DEBUG(logger, "erase from map," << " (SID)" << itr->second << " (UID)" << UID);
            UID_SID.erase(itr);
        }
    }
    //
    on_rtn_order(&rtn_order);
    raw_writer->write_frame(pOrder, sizeof(CQdpFtdcOrderField),
                            source_id, MSG_TYPE_LF_RTN_ORDER_QDP,
                            //1/*islast*/, (pOrder->RequestID > 0) ? pOrder->RequestID: -1);
                            1/*islast*/, (pOrder->RecNum > 0) ? pOrder->RecNum : -1);
}

void TDEngineQDP::OnRtnTrade(CQdpFtdcTradeField* pTrade)
{
    KF_LOG_INFO(logger, "[OnRtnTrade]" << " (TradeVolume) " << pTrade->TradeVolume);
    LFRtnTradeField rtn_trade = parseFrom(*pTrade);
    //scale_offset
    rtn_trade.Price = pTrade->TradePrice * scale_offset;
    rtn_trade.Volume = pTrade->TradeVolume * scale_offset;
    //
    on_rtn_trade(&rtn_trade);
    raw_writer->write_frame(pTrade, sizeof(CQdpFtdcTradeField),
                            source_id, MSG_TYPE_LF_RTN_TRADE_QDP, 1/*islast*/, -1/*invalidRid*/);
}

void TDEngineQDP::OnRspQryInvestorAccount(CQdpFtdcRspInvestorAccountField* pRspInvestorAccount, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast)
{
    int errorId = (pRspInfo == nullptr) ? 0 : pRspInfo->ErrorID;
    const char* errorMsg = (pRspInfo == nullptr) ? nullptr : EngineUtil::gbkErrorMsg2utf8(pRspInfo->ErrorMsg);
    CQdpFtdcRspInvestorAccountField empty = {0};
    if (pRspInvestorAccount == nullptr)
        pRspInvestorAccount = &empty;
    LFRspAccountField account = parseFrom(*pRspInvestorAccount);
    on_rsp_account(&account, bIsLast, nRequestID, errorId, errorMsg);
    raw_writer->write_error_frame(pRspInvestorAccount, sizeof(CQdpFtdcRspInvestorAccountField), source_id, MSG_TYPE_LF_RSP_ACCOUNT_QDP, bIsLast, nRequestID, errorId, errorMsg);
}

BOOST_PYTHON_MODULE(libqdptd)
{
    using namespace boost::python;
    class_<TDEngineQDP, boost::shared_ptr<TDEngineQDP> >("Engine")
    .def(init<>())
    .def("init", &TDEngineQDP::initialize)
    .def("start", &TDEngineQDP::start)
    .def("stop", &TDEngineQDP::stop)
    .def("logout", &TDEngineQDP::logout)
    .def("wait_for_stop", &TDEngineQDP::wait_for_stop);
}