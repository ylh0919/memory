#ifndef WINGCHUN_MDENGINEGOOGLETRENDS_H
#define WINGCHUN_MDENGINEGOOGLETRENDS_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include <vector>
#include <string>

WC_NAMESPACE_START

class MDEngineGoogletrends : public IMDEngine
{
public:
    /** load internal information from config json */
    virtual void load(const json& j_config);
    virtual void connect(long timeout_nsec);
    virtual void login(long timeout_nsec);
    virtual void logout();
    virtual void release_api();

    virtual bool is_connected() const { return connected; };
    virtual bool is_logged_in() const { return logged_in; };
    virtual string name() const { return "MDEngineGoogletrends"; };
    MDEngineGoogletrends();

private:
    void loop();
    inline int64_t getTimestamp();
    virtual void set_reader_thread() override;

    CoinPairWhiteList coinPairWhiteList;

    void getGoogleTrends(vector<string> keyword, string time);
    string replace_space(const string url);
    string set_token_url(vector<string> keyword, string time);
    string set_requset_url(string req, string token);
    string JsonToString(const rapidjson::Value& valObj);
    string set_timestr_hourly(int64_t timeNow);
    string set_timestr_daily(int64_t timeNow);
    string getdate(int64_t time);
    string gettime(int64_t time);
    string dealzero(string time);

    bool getToken(string url, string &req, string &token);
    bool getTimelineData(string url2, vector<string> keyword, string time);

private:
    ThreadPtr rest_thread;
    bool connected = false;
    bool logged_in = false;

    int trade_count = 10;
    int rest_get_interval_ms = 500;

    uint64_t last_rest_get_ts = 0;
    uint64_t last_trade_id = 0;
    static constexpr int scale_offset = 1e8;

    struct lws_context* context = nullptr;

    vector<string> whiteLists;
    int hourlyLoopTime = 3600;
    int dailyLoopTime = 86400;
};

DECLARE_PTR(MDEngineGoogletrends);

WC_NAMESPACE_END

#endif