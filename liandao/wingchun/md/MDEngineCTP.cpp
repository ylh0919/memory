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
 * MDEngineCTP: CTP's market data engine adapter.
 * @Author cjiang (changhao.jiang@taurus.ai)
 * @since   April, 2017
 */

#include "MDEngineCTP.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/ctp.h"
#include "longfist/LFUtils.h"
#include "CoinPairWhiteList.h"
USING_WC_NAMESPACE

MDEngineCTP::MDEngineCTP(): IMDEngine(SOURCE_CTP), api(nullptr), connected(false), logged_in(false), reqId(0)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.CTP");
    KF_LOG_INFO(logger, "MDEngineCTP::IMDEngine");

}

void MDEngineCTP::load(const json& j_config)
{
    KF_LOG_INFO(logger, "MDEngineCTP::load");
    
    broker_id = j_config["WC_CONFIG_KEY_BROKER_ID"].get<string>();
    user_id = j_config["WC_CONFIG_KEY_USER_ID"].get<string>();
    password = j_config["WC_CONFIG_KEY_PASSWORD"].get<string>();
    front_uri = j_config["WC_CONFIG_KEY_FRONT_URI"].get<string>();
    /*
    //load markets
    if (j_config.find("markets") != j_config.end()) {
        KF_LOG_INFO(logger, "MDEngineCTP::load markets");
        if (j_config["markets"].is_array())
            for (int i = 0; i < j_config["markets"].size(); i++)
                markets.push_back(j_config["markets"][i]);
        else if (j_config["markets"].is_string())
            markets.push_back(j_config["markets"].get<string>());
    }
    //load instruments
    if (j_config.find("whiteLists") != j_config.end()) {
        KF_LOG_INFO(logger, "MDEngineCTP::load whiteLists");
        m_whiteList.ReadWhiteLists(j_config, "whiteLists");
        m_whiteList.Debug_print();
        for (const auto& var : m_whiteList.GetKeyIsStrategyCoinpairWhiteList())
            instruments.push_back(var.second);
    }
    */
    /*
* modified kungfu.json like this
    "qdp": {
  "monitor_url": "ws://127.0.0.1:9997",
  "name": "md_qdp",
  "WC_CONFIG_KEY_BROKER_ID": "guofu",
  "WC_CONFIG_KEY_USER_ID": "08000027",
  "WC_CONFIG_KEY_PASSWORD": "123456",
  "WC_CONFIG_KEY_FRONT_URI": "tcp://180.168.221.66:30489",
  "whiteListsForExchange": {
    "CFFEX": {
      "if2103": "IF2103",
      "if2106": "IF2106"
    },
    "SHFE": {
      "pb2104": "pb2104",
      "au2106": "au2106"
    },
    "DCE": {
      "lh2111": "lh2111"
    },
    "CZCE": {
      "cj105": "CJ105",
      "cy105": "CY105"
    }
  }
},
*/
//load whiteListsForExchange
    if (j_config.find("whiteListsForExchange") != j_config.end()) {
        auto whiteList_config = j_config["whiteListsForExchange"];
        for (auto it = whiteList_config.begin(); it != whiteList_config.end(); it++) {
            string market = it.key();
            KF_LOG_INFO(logger, "load whiteListsForExchange, market: " << market.c_str());
            CoinPairWhiteList whitelist;
            whitelist.ReadWhiteLists(whiteList_config, market.c_str());
            whitelist.Debug_print();
            whiteListsForExchange.insert(make_pair(market, whitelist));
        }
    }
}

void MDEngineCTP::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineCTP::connect");
    if (api == nullptr)
    {
        KF_LOG_INFO(logger, "MDEngineCTP::connect api == nullptr");
        api = CThostFtdcMdApi::CreateFtdcMdApi();
        if (!api)
        {
            throw std::runtime_error("CTP_MD failed to create api");
        }
        api->RegisterSpi(this);
    }
    if (!connected)
    {
        KF_LOG_INFO(logger, "MDEngineCTP::connect !connected" << front_uri.c_str());
        api->RegisterFront((char*)front_uri.c_str());
        api->Init();
        long start_time = yijinjing::getNanoTime();
        while (!connected && yijinjing::getNanoTime() - start_time < timeout_nsec)
        {}
    }
    KF_LOG_ERROR(logger, "MDEngineCTP::end");
}

void MDEngineCTP::login(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineCTP::login");
    if (!logged_in)
    {
        CThostFtdcReqUserLoginField req = {};
        strcpy(req.BrokerID, broker_id.c_str());
        strcpy(req.UserID, user_id.c_str());
        strcpy(req.Password, password.c_str());
        if (api->ReqUserLogin(&req, reqId++))
        {
            KF_LOG_ERROR(logger, "[request] login failed!" << " (Bid)" << req.BrokerID
                                                           << " (Uid)" << req.UserID);
        }
        long start_time = yijinjing::getNanoTime();
        while (!logged_in && yijinjing::getNanoTime() - start_time < timeout_nsec)
        {}
    }
    //modified
    //if (instruments.size() != 0 && markets.size() != 0)
    //    subscribeMarketData(instruments, markets);
    //else
    //    KF_LOG_INFO(logger, "MDEngineCTP::instruments or markets is empty,add markets and whiteList in kungfu.json");
    if (whiteListsForExchange.size() != 0)
        subscribeMarketData();
    else
        KF_LOG_INFO(logger, "instruments or markets is empty,add info of markets and whiteList in kungfu.json"); 
}

void MDEngineCTP::logout()
{
    KF_LOG_INFO(logger, "MDEngineCTP::logout");
    if (logged_in)
    {
        CThostFtdcUserLogoutField req = {};
        strcpy(req.BrokerID, broker_id.c_str());
        strcpy(req.UserID, user_id.c_str());
        if (api->ReqUserLogout(&req, reqId++))
        {
            KF_LOG_ERROR(logger, "[request] logout failed!" << " (Bid)" << req.BrokerID
                                                            << " (Uid)" << req.UserID);
        }
    }
    connected = false;
    logged_in = false;
}

void MDEngineCTP::release_api()
{
    KF_LOG_INFO(logger, "MDEngineCTP::release_api");
    if (api != nullptr)
    {
        api->Release();
        api = nullptr;
    }
}

void MDEngineCTP::subscribeMarketData()
{
    KF_LOG_INFO(logger, "MDEngineCTP::subscribeMarketData by whiteListsForExchange");
    for (auto itr = whiteListsForExchange.begin(); itr != whiteListsForExchange.end(); itr++) {
        string market = itr->first;
        vector<string> instruments;

        int nCount = itr->second.Size();
        auto whitelist = itr->second.GetKeyIsStrategyCoinpairWhiteList();
        char* insts[nCount];
        int i = 0;
        for (const auto& var : itr->second.GetKeyIsStrategyCoinpairWhiteList()) {
            insts[i] = (char*)var.second.c_str();
            i++;
        }
        KF_LOG_INFO(logger, "subscribeMarketData, nCount: " << nCount);
        KF_LOG_INFO(logger, "subscribeMarketData, market: " << market.c_str());
        api->SubscribeMarketData(insts, nCount);
    }
}

void MDEngineCTP::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineCTP::subscribeMarketData");
    int nCount = instruments.size();
    char* insts[nCount];
    for (int i = 0; i < nCount; i++)
        insts[i] = (char*)instruments[i].c_str();
    api->SubscribeMarketData(insts, nCount);
}

/*
 * SPI functions
 */
void MDEngineCTP::OnFrontConnected()
{
    KF_LOG_INFO(logger, "[OnFrontConnected]");
    connected = true;
}

void MDEngineCTP::OnFrontDisconnected(int nReason)
{
    KF_LOG_INFO(logger, "[OnFrontDisconnected] reason=" << nReason);
    connected = false;
    logged_in = false;
}

#define GBK2UTF8(msg) kungfu::yijinjing::gbk2utf8(string(msg))

void MDEngineCTP::OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInfo != nullptr && pRspInfo->ErrorID != 0)
    {
        KF_LOG_ERROR(logger, "[OnRspUserLogin]" << " (errID)" << pRspInfo->ErrorID
                                                << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg));
    }
    else
    {
        KF_LOG_INFO(logger, "[OnRspUserLogin]" << " (Bid)" << pRspUserLogin->BrokerID
                                               << " (Uid)" << pRspUserLogin->UserID
                                               << " (SName)" << pRspUserLogin->SystemName);
        logged_in = true;
    }
}

void MDEngineCTP::OnRspUserLogout(CThostFtdcUserLogoutField *pUserLogout, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInfo != nullptr && pRspInfo->ErrorID != 0)
    {
        KF_LOG_ERROR(logger, "[OnRspUserLogout]" << " (errID)" << pRspInfo->ErrorID
                                                 << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg));
    }
    else
    {
        KF_LOG_INFO(logger, "[OnRspUserLogout]" << " (Bid)" << pUserLogout->BrokerID
                                                << " (Uid)" << pUserLogout->UserID);
        logged_in = false;
    }
}

void MDEngineCTP::OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument, CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    KF_LOG_INFO(logger, "MDEngineCTP::OnRspSubMarketData");
    if (pRspInfo != nullptr && pRspInfo->ErrorID != 0)
    {
        KF_LOG_ERROR(logger, "[OnRspSubMarketData]" << " (errID)" << pRspInfo->ErrorID
                                                    << " (errMsg)" << GBK2UTF8(pRspInfo->ErrorMsg)
                                                    << " (Tid)" << ((pSpecificInstrument != nullptr) ?
                                                                    pSpecificInstrument->InstrumentID : "null"));
    }
}

void MDEngineCTP::OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData)
{
    KF_LOG_INFO(logger, "MDEngineCTP::OnRtnDepthMarketData");
    // auto data = parseFrom(*pDepthMarketData);
    //BidPrice BidVolume AskPrice AskVolume *= 1e8
    auto data = parsewWithScale_offset(*pDepthMarketData);

    //KF_LOG_INFO(logger, "[OnRtnDepthMarketData] pDepthMarketData" << " (AskPrice5)" << pDepthMarketData->AskPrice5 << " (BidPrice5)" << pDepthMarketData->BidPrice5);
    //KF_LOG_INFO(logger, "[OnRtnDepthMarketData] data            " << " (AskPrice5)" << data.AskPrice5 << " (BidPrice5)" << data.BidPrice5);
    //check data
    const double limit = DBL_MAX - 1e8;
    if (data.AskVolume1 == 0 || pDepthMarketData->AskPrice1 > limit)
        data.AskPrice1 = 0;
    if (data.AskVolume2 == 0 || pDepthMarketData->AskPrice2 > limit)
        data.AskPrice2 = 0;
    if (data.AskVolume3 == 0 || pDepthMarketData->AskPrice3 > limit)
        data.AskPrice3 = 0;
    if (data.AskVolume4 == 0 || pDepthMarketData->AskPrice4 > limit)
        data.AskPrice4 = 0;
    if (data.AskVolume5 == 0 || pDepthMarketData->AskPrice5 > limit)
        data.AskPrice5 = 0;

    if (data.BidVolume1 == 0 || pDepthMarketData->BidPrice1 > limit)
        data.BidPrice1 = 0;
    if (data.BidVolume2 == 0 || pDepthMarketData->BidPrice2 > limit)
        data.BidPrice2 = 0;
    if (data.BidVolume3 == 0 || pDepthMarketData->BidPrice3 > limit)
        data.BidPrice3 = 0;
    if (data.BidVolume4 == 0 || pDepthMarketData->BidPrice4 > limit)
        data.BidPrice4 = 0;
    if (data.BidVolume5 == 0 || pDepthMarketData->BidPrice5 > limit)
        data.BidPrice5 = 0;

    on_market_data(&data);
    // if need to write raw data...
    // raw_writer->write_frame(pDepthMarketData, sizeof(CThostFtdcDepthMarketDataField),
    //                         source_id, MSG_TYPE_LF_MD_CTP, 1/*islast*/, -1/*invalidRid*/);
}

BOOST_PYTHON_MODULE(libctpmd)
{
    using namespace boost::python;
    class_<MDEngineCTP, boost::shared_ptr<MDEngineCTP> >("Engine")
    .def(init<>())
    .def("init", &MDEngineCTP::initialize)
    .def("start", &MDEngineCTP::start)
    .def("stop", &MDEngineCTP::stop)
    .def("logout", &MDEngineCTP::logout)
    .def("wait_for_stop", &MDEngineCTP::wait_for_stop);
}