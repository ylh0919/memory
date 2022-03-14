//
// Created by wang on 10/20/18.
//

#ifndef KUNGFU_MDENGINEHBDM_H
#define KUNGFU_MDENGINEHBDM_H
#include "IMDEngine.h"
#include <map>
#include <vector>
#include <queue>
#include <time.h>
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"
#include "../base/ThreadPool.h"
struct lws_context;
struct lws;

WC_NAMESPACE_START
class MDEngineHbdm:public IMDEngine
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
    void onMessage(struct lws*,char* , size_t );
    void onClose(struct lws*);
    void onWrite(struct lws*);
    void reset();
protected:
    //url format is xxx://xxx.xxx.xxx:xxx/
    bool parseAddress(const std::string& exch_url);
    void parsePingMsg(const rapidjson::Document&,struct lws* conn);
    void parseRspSubscribe(const rapidjson::Document&);
    void parseSubscribeData(const rapidjson::Document&);
    void set_reader_thread() override;
    void doDepthData(const rapidjson::Document&, const std::string&);
    void doTradeData(const rapidjson::Document&, const std::string&);
    void doKlineData(const rapidjson::Document&, const std::string&);
    void doKlineData(const rapidjson::Document&, const std::string&, const std::string&);
    int64_t  GetMillsecondByInterval(const std::string& interval);
    void handle_lws_data(struct lws* conn, std::string data);
private:
    void genSubscribeString();
    std::string genDepthString(const std::string&);
    std::string genTradeString(const std::string&);
    std::string genKlineString(const std::string&);
    std::string genKlineString(const std::string&, const std::string&);
    std::string getIntervalStr(int64_t sec);
    std::string genPongString(const std::string&);
    inline int64_t getTimestamp();

private:
    void createConnection();
    void lwsEventLoop();
    void sendMessage(std::string&& );
    void get_snapshot_via_rest();
    void rest_loop();
private:
    bool                        m_connected = false;
    bool                        m_logged_in = false;
    ThreadPtr                   m_thread;
    ThreadPtr                   rest_thread;
    int                         rest_get_interval_ms = 500;
private:
    CoinPairWhiteList           m_whiteList;
    std::vector<std::string>    m_subcribeJsons;
    std::queue<std::string>     pong_queue;
    int                         m_subcribeIndex = 0;
    int64_t                     m_id = 0;
    int                         m_priceBookNum = 20;
    /*edited by zyy,starts here*/
    int                         level_threshold = 15;
    int                         refresh_normal_check_book_s = 120;
    int64_t                     timer;
    /*edited by zyy ends here*/
    std::map<std::string,std::map<int64_t,LFBarMarketDataField>> mapKLines;
private:
    struct lws_context*         m_lwsContext = nullptr;
    struct lws*                 m_lwsConnection = nullptr;
    UrlInfo                     m_exchUrl;
private:
    MonitorClientPtr            m_monitorClient;
    ThreadPool* m_ThreadPoolPtr = nullptr;
private:
    //subscribe two kline
    string kline_a_interval = "1min";
    string kline_b_interval = "1min";
};
DECLARE_PTR(MDEngineHbdm);
WC_NAMESPACE_END
#endif //KUNGFU_MDENGINEHBDM_H
