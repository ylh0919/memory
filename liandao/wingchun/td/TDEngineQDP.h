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

#ifndef PROJECT_TDENGINEQDP_H
#define PROJECT_TDENGINEQDP_H

#include "ITDEngine.h"
#include "longfist/LFConstants.h"
#include "QdpFtdcTraderApi.h"
#include "CoinPairWhiteList.h"

WC_NAMESPACE_START

/**
 * account information unit extra for QDP is here.
 */
struct AccountUnitQDP
{
    // api
    CQdpFtdcTraderApi* api;
    // extra information
    string  app_id;
    string  user_product_info;
    string  auth_code;
    int     front_id;
    int     session_id;
    // internal flags
    bool    initialized;
    bool    connected;
    bool    authenticated;
    //bool    settle_confirmed;
    bool    logged_in;
    // some rids
    int     auth_rid;
    int     settle_rid;
    int     login_rid;

    CoinPairWhiteList coinPairWhiteList;
    CoinPairWhiteList positionWhiteList;
};

/**
 * QDP trade engine
 */
class TDEngineQDP: public ITDEngine, public CQdpFtdcTraderSpi
{
public:
    /** init internal journal writer (both raw and send) */
    virtual void init();
    /** for settleconfirm and authenticate setting */
    virtual void pre_load(const json& j_config);
    virtual TradeAccount load_account(int idx, const json& j_account);
    virtual void resize_accounts(int account_num);
    /** connect && login related */
    virtual void connect(long timeout_nsec);
    virtual void login(long timeout_nsec);
    virtual void logout();
    virtual void release_api();
    virtual bool is_connected() const;
    virtual bool is_logged_in() const;
    virtual string name() const { return "TDEngineQDP"; };

    // req functions
    virtual void req_investor_position(const LFQryPositionField* data, int account_index, int requestId);
    virtual void req_qry_account(const LFQryAccountField* data, int account_index, int requestId);
    virtual void req_order_insert(const LFInputOrderField* data, int account_index, int requestId, long rcv_time);
    virtual void req_order_action(const LFOrderActionField* data, int account_index, int requestId, long rcv_time);

public:
    TDEngineQDP();

private:
    // journal writers
    yijinjing::JournalWriterPtr raw_writer;
    // from config
    string front_uri;
    //bool need_settleConfirm;
    bool need_authenticate;
    int curAccountIdx;
    vector<string> markets;
    vector<AccountUnitQDP> account_units;
    static constexpr int scale_offset = 1e8;
    //first: LF.OrderRef/QDP.UserOrderLocalID and OrderSysID
    map<string, string> UID_SID;
public:
    // SPI
    ///当客户端与交易后台建立起通信连接时（还未登录前），该方法被调用。
    virtual void OnFrontConnected();

    ///当客户端与交易后台通信连接断开时，该方法被调用。当发生这个情况后，API会自动重新连接，客户端可不做处理。
    ///@param nReason 错误原因
    ///        0x1001 网络读失败
    ///        0x1002 网络写失败
    ///        0x2001 接收心跳超时
    ///        0x2002 发送心跳失败
    ///        0x2003 收到错误报文
    virtual void OnFrontDisconnected(int nReason);

    ///客户端认证响应
    virtual void OnRspAuthenticate(CQdpFtdcRtnAuthenticateField* pRtnAuthenticate, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast);

    ///登录请求响应
    virtual void OnRspUserLogin(CQdpFtdcRspUserLoginField* pRspUserLogin, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast);

    ///投资者结算结果确认响应
    //virtual void OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField *pSettlementInfoConfirm, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);
    //not found

    ///登出请求响应
    virtual void OnRspUserLogout(CQdpFtdcRspUserLogoutField* pRspUserLogout, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast);

    ///报单录入请求响应 (cjiang: this only be called when there is error)
    virtual void OnRspOrderInsert(CQdpFtdcInputOrderField* pInputOrder, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast);

    ///报单操作请求响应
    virtual void OnRspOrderAction(CQdpFtdcOrderActionField* pOrderAction, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast);

    ///请求查询投资者持仓响应
    virtual void OnRspQryInvestorPosition(CQdpFtdcRspInvestorPositionField* pRspInvestorPosition, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast);

    ///报单通知
    virtual void OnRtnOrder(CQdpFtdcOrderField* pOrder);

    ///成交通知
    virtual void OnRtnTrade(CQdpFtdcTradeField* pTrade);

    ///请求查询资金账户响应
    //virtual void OnRspQryTradingAccount(CThostFtdcTradingAccountField *pTradingAccount, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast);
    // not found
    //may try this
    ///投资者资金账户查询应答
    virtual void OnRspQryInvestorAccount(CQdpFtdcRspInvestorAccountField* pRspInvestorAccount, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) ;

    //可用投资者账户查询应答
    void OnRspQryUserInvestor(CQdpFtdcRspUserInvestorField* pRspUserInvestor, CQdpFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast);
};

WC_NAMESPACE_END

#endif //PROJECT_TDENGINEQDP_H
