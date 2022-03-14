// https://ascendex.github.io/ascendex-pro-api/
#ifndef KUNGFU_MDENGINEASCENDEX_H
#define KUNGFU_MDENGINEASCENDEX_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include <libwebsockets.h>
#include <map>
#include <vector>
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"
struct lws_context;
struct lws;

WC_NAMESPACE_START
class MDEngineAscendEX: public IMDEngine
{
public:
    static MDEngineAscendEX*       m_instance;
public:
    MDEngineAscendEX();
    virtual ~MDEngineAscendEX();

public:
    void load(const json& ) override;
    void connect(long ) override;
    void login(long ) override;
    void logout() override;
    void release_api() override { KF_LOG_INFO(logger, "release_api"); }
    bool is_connected() const override { return m_connected; }
    bool is_logged_in() const override { return m_logged_in; }
    std::string name()  const  override { return "MDEngineAscendEX"; }
    void subscribeMarketData(const std::vector<std::string>& , const std::vector<std::string>& ) override{}

public:
    void onMessage(struct lws*,char* , size_t );
    void onClose(struct lws*);
    void onWrite(struct lws*);
    void reset();
protected:
    //url format is xxx://xxx.xxx.xxx:xxx/
    //bool parseAddress(const std::string& exch_url);
    void parsePingMsg(const rapidjson::Document&);
    void parseRspSubscribe(const rapidjson::Document&);
    void parseErrSubscribe(const rapidjson::Document&);
    void parseSubscribeData(const rapidjson::Document&, const std::string&);
    void set_reader_thread() override;
    void doDepthData(const rapidjson::Document&, const std::string&);
    void doTradeData(const rapidjson::Document&, const std::string&);
    void doKlineData(const rapidjson::Document&, const std::string&);

private:
    void genSubscribeString();
    std::string genDepthString(const std::string&);
    std::string genTradeString(const std::string&);
    std::string genKlineString(const std::string&);
    std::string genKlineString(const std::string&, int);
    std::string genPongString();

private:
    void createConnection();
    void lwsEventLoop();
    void sendMessage(std::string&& );
    inline int64_t getTimestamp();

private:
    void get_snapshot_via_rest();
    void rest_loop();

private:
    int                         accountGroup = 0;
    string                      restBaseUrl = "ascendex.com";
    string                      wsBaseUrl = "ascendex.com/0/api/pro/v1/stream";
private:
    bool                        m_connected = false;
    bool                        m_logged_in = false;
    ThreadPtr                   m_thread;
private:
    CoinPairWhiteList           m_whiteList;
    std::vector<std::string>    m_subcribeJsons;
    int                         m_subcribeIndex = 0;
    int64_t                     m_id = 0;
    int                         m_priceBookNum = 20;
    static constexpr int        scale_offset = 1e8;
private:
    struct lws_context*         m_lwsContext = nullptr;
    struct lws*                 m_lwsConnection = nullptr;
    UrlInfo                     m_exchUrl;
private:
    MonitorClientPtr            m_monitorClient;
    int                         level_threshold = 15;
    int                         refresh_normal_check_book_s = 120;
    int64_t                     timer;
    std::map<std::string, std::map<int64_t, LFBarMarketDataField>> mapKLines;
private:
    bool                        need_get_snapshot_via_rest = false;
    int                         rest_get_interval_ms = 500;
    ThreadPtr                   rest_thread;
private:
    //subscribe two kline, min
    int kline_a_interval = 1;
    int kline_b_interval = 1;
};
DECLARE_PTR(MDEngineAscendEX);
WC_NAMESPACE_END
#endif //KUNGFU_MDENGINEASENDEX_H