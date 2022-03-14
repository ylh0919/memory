//
// Created by wang on 10/20/18.
//

#ifndef KUNGFU_MDENGINEHBDM_H
#define KUNGFU_MDENGINEHBDM_H
#include "IMDEngine.h"
#include <map>
#include <mutex>
#include <vector>
#include <queue>
#include <time.h>
#include <websocket.h>
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"
#include "../base/ThreadPool.h"
struct lws_context;
struct lws;

WC_NAMESPACE_START
struct BookMsg
{
    std::string InstrumentID;
    int64_t time;
    uint64_t sequence;
};

struct WsMsg
{
    bool is_swap;
    std::string ws_url;
    common::CWebsocket websocket;
    bool is_ws_disconnectd = false;
    bool logged_in;
    /*struct lws_context*         m_lwsContext = nullptr;
    struct lws*                 m_lwsConnection = nullptr;
    UrlInfo                     m_exchUrl;*/
};

class MDEngineHbdm:public IMDEngine,public common::CWebsocketCallBack
{
public:
    static MDEngineHbdm*       m_instance;
public:
    MDEngineHbdm();
    virtual ~MDEngineHbdm();

public:
    void load(const json& ) override;
    void connect(long ) override;
    void login(long ) override;
    void logout() override;
    void release_api() override { KF_LOG_INFO(logger, "release_api"); }
    bool is_connected() const override { return m_connected; }
    bool is_logged_in() const override { return m_logged_in; }
    std::string name() const  override { return "MDEngineHbdm"; }
    void subscribeMarketData(const std::vector<std::string>& , const std::vector<std::string>& ) override{}

public:
    void onClose(struct lws*);
    void onWrite(struct lws*);
    void reset();
protected:
    void parsePingMsg(const rapidjson::Document& json,const common::CWebsocket* instance);
    void parseRspSubscribe(const rapidjson::Document&);
    void parseSubscribeData(const rapidjson::Document&);
    void set_reader_thread() override;
    void doDepthData(const rapidjson::Document&, const std::string&);
    void doTradeData(const rapidjson::Document&, const std::string&);
    void doKlineData(const rapidjson::Document&, const std::string&);
    void handle_lws_data(const common::CWebsocket* instance, std::string data);
private:
    void genSubscribeString();
    void makeWebsocketSubscribeJsonString(const common::CWebsocket* instance);
    std::string genDepthString(const std::string&);
    std::string genTradeString(const std::string&);
    std::string genKlineString(const std::string&);
    std::string genPongString(const std::string&);
    inline int64_t getTimestamp();

    virtual void OnConnected(const common::CWebsocket* instance);
    virtual void OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg);
    virtual void OnDisconnected(const common::CWebsocket* instance);    

private:
    void lws_login(WsMsg wsmsg);
    void createConnection(WsMsg wsmsg);
    void lwsEventLoop();
    void sendMessage(std::string&& msg, struct lws* conn);
    void get_snapshot_via_rest();
    void rest_loop();
    void check_snapshot();
    void check_loop();
private:
    bool                        m_connected = false;
    bool                        m_logged_in = false;
    //bool                        is_ws_disconnectd = false;
    ThreadPtr                   m_thread;
    ThreadPtr                   rest_thread;
    ThreadPtr                   check_thread;
    int                         rest_get_interval_ms = 500;
    int                         snapshot_check_s = 20;
private:
    CoinPairWhiteList           m_whiteList;
    std::map<std::string, int64_t> control_trade_map;
    std::map<std::string, int64_t> control_book_map;
    std::map<std::string, int64_t> control_kline_map;

    std::vector<std::string>    m_subcribeJsons;
    std::queue<std::string>     pong_queue;
    int                         m_subcribeIndex = 0;
    int64_t                     m_id = 0;
    int                         m_priceBookNum = 20;
    /*edited by zyy,starts here*/
    int                         level_threshold = 15;
    int                         refresh_normal_check_book_s = 120;
    int                         refresh_normal_check_trade_s = 120;
    int                         refresh_normal_check_kline_s = 120;
    int64_t                     timer;
    /*edited by zyy ends here*/
    std::map<std::string,std::map<int64_t,LFBarMarketDataField>> mapKLines;
    std::map<std::string, std::map<uint64_t, int64_t>> ws_book_map;
    std::vector<BookMsg> rest_book_vec;
    std::vector<WsMsg> ws_msg_vec;
/*private:
    struct lws_context*         m_lwsContext = nullptr;
    struct lws*                 m_lwsConnection = nullptr;
    UrlInfo                     m_exchUrl;*/

private:
    MonitorClientPtr            m_monitorClient;
    ThreadPool* m_ThreadPoolPtr = nullptr;
};
DECLARE_PTR(MDEngineHbdm);
WC_NAMESPACE_END
#endif //KUNGFU_MDENGINEHBDM_H
