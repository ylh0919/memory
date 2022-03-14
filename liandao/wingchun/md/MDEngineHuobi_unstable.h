//
// Created by wang on 10/20/18.
//

#ifndef KUNGFU_MDENGINEHUOBI_H
#define KUNGFU_MDENGINEHUOBI_H
#include "IMDEngine.h"
#include <map>
#include <vector>
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"
struct lws_context;
struct lws;

WC_NAMESPACE_START
struct BookMsg
{
    std::string InstrumentID;
    int64_t time;
    uint64_t sequence;
};

class MDEngineHuobi:public IMDEngine
{
public:
    static MDEngineHuobi*       m_instance;
public:
    MDEngineHuobi();
    virtual ~MDEngineHuobi();

public:
    void load(const json& ) override;
    void connect(long ) override;
    void login(long ) override;
    void logout() override;
    void release_api() override { KF_LOG_INFO(logger, "release_api"); }
    bool is_connected() const override { return m_connected; }
    bool is_logged_in() const override { return m_logged_in; }
    std::string name() const  override { return "MDEngineHuobi"; }
    void subscribeMarketData(const std::vector<std::string>& , const std::vector<std::string>& ) override{}

public:
    void onMessage(struct lws*,char* , size_t );
    void onClose(struct lws*);
    void onWrite(struct lws*);
    void reset();
protected:
    //url format is xxx://xxx.xxx.xxx:xxx/
    bool parseAddress(const std::string& exch_url);
    void parsePingMsg(const rapidjson::Document&);
    void parseRspSubscribe(const rapidjson::Document&);
    void parseSubscribeData(const rapidjson::Document&);
    void set_reader_thread() override;
    void doDepthData(const rapidjson::Document&, const std::string&);
    void doTradeData(const rapidjson::Document&, const std::string&);

private:
    void genSubscribeString();
    std::string genDepthString(const std::string&);
    std::string genTradeString(const std::string&);
    std::string genPongString(const std::string&);

private:
    void createConnection();
    void lwsEventLoop();
    void sendMessage(std::string&& );
    inline int64_t getTimestamp();
    void check_snapshot();
    void get_snapshot_via_rest();
    void rest_loop();
    void check_loop();

private:
    bool                        m_connected = false;
    bool                        m_logged_in = false;
    ThreadPtr                   m_thread;
    ThreadPtr                   rest_thread;
    ThreadPtr                   check_thread;
private:
    CoinPairWhiteList           m_whiteList;

    std::map<std::string, int64_t> control_trade_map;
    std::map<std::string, int64_t> control_book_map;
    std::map<std::string, int64_t> control_kline_map;

    std::vector<std::string>    m_subcribeJsons;
    int                         m_subcribeIndex = 0;
    int64_t                     m_id = 0;
    int                         m_priceBookNum = 20;
    int                         snapshot_check_s = 20;
    int                         rest_get_interval_ms = 500;
private:
    struct lws_context*         m_lwsContext = nullptr;
    struct lws*                 m_lwsConnection = nullptr;
    UrlInfo                     m_exchUrl;
private:
    MonitorClientPtr            m_monitorClient;
    /*edited by zyy,starts here*/
    int                         level_threshold = 15;
    int                         refresh_normal_check_book_s = 120;
    int                         refresh_normal_check_trade_s = 120;
    int64_t                     timer;
    /*edited by zyy ends here*/
    std::map<std::string, std::map<uint64_t, int64_t>> ws_book_map;
    std::vector<BookMsg> rest_book_vec;
};
DECLARE_PTR(MDEngineHuobi);
WC_NAMESPACE_END
#endif //KUNGFU_MDENGINEHUOBI_H
