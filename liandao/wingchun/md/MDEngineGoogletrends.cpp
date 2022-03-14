#include "MDEngineGoogletrends.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"

#include <ctime>
#include <writer.h>
#include <stringbuffer.h>

#include <document.h>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <string>
#include <cpr/cpr.h>
#include <chrono>
#include <algorithm>

using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;

using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;

USING_WC_NAMESPACE

static MDEngineGoogletrends* g_md_googletrends = nullptr;

MDEngineGoogletrends::MDEngineGoogletrends() : IMDEngine(SOURCE_GOOGLETRENDS)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Googletrends");
}

void MDEngineGoogletrends::connect(long timeout_nsec)
{
	KF_LOG_INFO(logger, "MDEngineGoogletrends::connect set connected = true");
	connected = true;
}

void MDEngineGoogletrends::load(const json& j_config)
{
    KF_LOG_INFO(logger, "MDEngineGoogletrends::load");
    if (j_config.find("whiteLists") != j_config.end()) {
        KF_LOG_INFO(logger, "MDEngineGoogletrends::load whiteLists");
        json listJson = j_config["whiteLists"].get<json>();
        if (listJson.is_array()) {
            for (json::iterator itr = listJson.begin(); itr != listJson.end(); itr++) {
                string kw = *itr;
                KF_LOG_INFO(logger, "MDEngineGoogletrends::load whiteLists: " << kw);
                whiteLists.push_back(kw);
            }
        }
    }

    if (j_config.find("hourlyLoopTime") != j_config.end())
        hourlyLoopTime = j_config["hourlyLoopTime"].get<int>();
    if(j_config.find("dailyLoopTime") != j_config.end())
        dailyLoopTime = j_config["hourlyLoopTime"].get<int>();
}

void MDEngineGoogletrends::login(long timeout_nsec)
{
    g_md_googletrends = this;
    KF_LOG_INFO(logger, "MDEngineGoogletrends::login nothing to do");
    logged_in = true;
}

void MDEngineGoogletrends::logout()
{
    KF_LOG_INFO(logger, "MDEngineGoogletrends::logout nothing to do");
}

void MDEngineGoogletrends::release_api()
{
    KF_LOG_INFO(logger, "MDEngineGoogletrends::release_api nothing to do");
}

void MDEngineGoogletrends::set_reader_thread()
{
    KF_LOG_INFO(logger, "MDEngineGoogletrends::set_reader_thread");
    IMDEngine::set_reader_thread();
    rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineGoogletrends::loop, this)));
}

void MDEngineGoogletrends::loop()
{
    KF_LOG_INFO(logger, "MDEngineGoogletrends::loop" << isRunning);

    vector<string> keyword;
    string hourlyTimestr = "now+7-d";
    string dailyTimestr = "today+3-m";

    while (isRunning) {
        int64_t timeNow = time(0);
        if (timeNow % hourlyLoopTime == 0) {
            KF_LOG_INFO(logger, "timeNow: " << timeNow);
            KF_LOG_INFO(logger, "MDEngineGoogletrends::loop hourly query" << isRunning);
            getGoogleTrends(whiteLists, hourlyTimestr);
            //getGoogleTrends(whiteLists, set_timestr_hourly(timeNow));
            if (timeNow % dailyLoopTime == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                KF_LOG_INFO(logger, "MDEngineGoogletrends::loop daily query" << isRunning);
                getGoogleTrends(whiteLists, dailyTimestr);
                //getGoogleTrends(whiteLists, set_timestr_daily(timeNow));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }
}

inline int64_t MDEngineGoogletrends::getTimestamp()
{   /*·µ»ØµÄÊÇºÁÃë*/
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}

string MDEngineGoogletrends::dealzero(std::string time)
{
    if (time.length() == 1) {
        time = "0" + time;
    }
    return time;
}

string MDEngineGoogletrends::gettime(int64_t time)
{
    time_t now = time;
    tm* ltm = localtime(&now);
    std::string hour = std::to_string(ltm->tm_hour);
    hour = dealzero(hour);
    std::string min = std::to_string(ltm->tm_min);
    min = dealzero(min);
    std::string sec = std::to_string(ltm->tm_sec);
    sec = dealzero(sec);
    std::string timestr = hour + min + sec;
    return timestr;
}

string MDEngineGoogletrends::getdate(int64_t time)
{
    time_t now = time;
    tm* ltm = localtime(&now);
    std::string year = std::to_string(1900 + ltm->tm_year);
    std::string month = std::to_string(1 + ltm->tm_mon);
    month = dealzero(month);
    std::string date = std::to_string(ltm->tm_mday);
    date = dealzero(date);
    std::string datestr = year + "-" + month + "-" + date;
    return datestr;
}

//180day in daily
string MDEngineGoogletrends::set_timestr_daily(int64_t timeNow) {
    int64_t timeBefore = timeNow - (24 * 60 * 60) * 180;
    string dateNow = getdate(timeNow);
    string dateBefore = getdate(timeBefore);
    string timestr = dateBefore + " " + dateNow;
    return timestr;
}

//7days in hourly
string MDEngineGoogletrends::set_timestr_hourly(int64_t timeNow) {
    int64_t timeBefore = timeNow - (24 * 60 * 60) * 7;
    string dateNow = getdate(timeNow);
    string dateBefore = getdate(timeBefore);
    string timestr = dateBefore + " " + dateNow;
    return timestr;
}

string MDEngineGoogletrends::JsonToString(const rapidjson::Value& valObj)
{
    rapidjson::StringBuffer sbBuf;
    rapidjson::Writer<rapidjson::StringBuffer> jWriter(sbBuf);
    valObj.Accept(jWriter);
    return std::string(sbBuf.GetString());
}

string MDEngineGoogletrends::replace_space(const string url) {
    if (url.size() == 0)
        return url;

    string res = url;
    while (true) {
        int pos = res.find(" ");
        if (pos == string::npos)
            break;
        else {
            res.replace(pos, 1, "%20");
        }
    }
    return res;
}

string MDEngineGoogletrends::set_requset_url(string req, string token) {
    //string url = "https://trends.google.com/trends/api/widgetdata/multiline?hl=zh-CN&tz=-480&req=" +
    string url = "https://trends.google.com/trends/api/widgetdata/multiline?hl=en-US&tz=-480&req=" +
        req + "&token=" +
        token;
    return url;
}

string MDEngineGoogletrends::set_token_url(vector<string> keyword, string time) {
    string url = "https://trends.google.com/trends/api/explore?hl=en-US&tz=-480&req=%7B%22comparisonItem%22:%5B";
    bool firstline = true;
    for (auto itr : keyword) {
        if (firstline) {
            url = url + "%7B%22keyword%22:%22" + itr + "%22,%22geo%22:%22%22,%22time%22:%22" + time + "%22%7D";
            firstline = false;
        }
        else
            url = url + ",%7B%22keyword%22:%22" + itr + "%22,%22geo%22:%22%22,%22time%22:%22" + time + "%22%7D";
    }
    url += "%5D,%22category%22:0,%22property%22:%22%22%7D";
    return url;
}

bool MDEngineGoogletrends::getToken(string url, string &req, string &token) {
    cpr::Response response = Get(Url{ url }
        //cpr::Response response = Get(Url{ "https://trends.google.com/trends/api/explore?tz=-480&req={%22comparisonItem%22:[{%22keyword%22:%22bitcoin,USDT,BTC,Ethereum%22,%22time%22:%222020-05-18%202020-11-14%22}]}" }
        , cpr::Header{
            {"user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"},
            {"x-client-data", "CJW2yQEIo7bJAQjEtskBCKmdygEIq8fKAQj1x8oBCOnIygEItMvKAQjb1coBCKLWygEIl5rLAQjBmssBGIrBygE="},
            {"cookie", "__utmc=10102256; __utmz=10102256.1604910310.2.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utma=10102256.1148352310.1604907731.1604910829.1605344577.8; CONSENT=YES+CN.zh-CN+202007; SEARCH_SAMESITE=CgQInpAB; __Secure-3PAPISID=Trz6pkCblVViT1ty/AN0A-F-fHN-dql_ik; SSID=AAXMTJ3PjQ2GDlFAJ; SAPISID=Trz6pkCblVViT1ty/AN0A-F-fHN-dql_ik; HSID=AnFtRnCH73Tudlhag; APISID=VNgOW-9vnIvi_EHq/AOr79yEu3Z3kOH1kU; __Secure-3PSID=3Qdi_3ZDiaeH8_g7rbHuW8Y6bztGZ0Kj-Sb26phT9mQ2BtuR8TiasJJckrIvtHyqO1580Q.; SID=3Qdi_3ZDiaeH8_g7rbHuW8Y6bztGZ0Kj-Sb26phT9mQ2BtuRB9LGT3nwjGrpLs6xZlflCw.; ANID=AHWqTUlt2Qcs1kD27OjBq7JuOHjvmUKp0QBtdg5OHRQCOLxeoSQy6wyUUxzEAxSa; 1P_JAR=2020-11-14-08; NID=204=xBa9NggKyoO1aSGJExvSWxTQ9aBYQCEr2qeyU7FGx3WuKJCstPjTRUtgr6P7r2ppVxpGJCW2OdkxHqvRcY47NTKmkPG2gWn-8dNWXXQd6QLRhQI3guqA84zPDEil9tnaNzwF4p2clJmuRse5G1k-v_Fl3RCqXFGqVQfh5ylXCTCJy0oZh31LMra60-MGFZlmVIJrO3Z8HHfIwgZEcv5hPpdP873w1qUibGnXukjJNvXgIy-VFD6eF_uMUzHndiDwAsfgnM7iCEoN_2OZhyelTwQJSwy-kuMUYhcm4GT_4uiuNoho177_GoN5E2wl028_88_MT97qJS3f36DgN_R7pTCbr4bcczfZZo7WPOmjDHVc6PSXvEDV6SyYsKyVE3ecCsg; SIDCC=AJi4QfGlwB1ViYrnkcVmf2JYm-1P1ZXOuL_i1I6asTWLJVFo59acmgo_d99dCYkOi4T4pxfvVfM; __Secure-3PSIDCC=AJi4QfHxaGNTiGPitZYKWjEZM_mgJ6hE_EJ-IzqN6U5PpBEvIhgrVNwp-7ufmvmNA_I1DOhv9Qn-"},
            {"accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"}
        }, cpr::Timeout{ 1000 });
    
    if (response.status_code >= 400) {
        KF_LOG_INFO(logger, "URL1 Error [" << response.status_code << "] making request ");
        KF_LOG_INFO(logger, "Request took " << response.elapsed);
        KF_LOG_INFO(logger, "Body: " << response.text);

        string errMsg;
        errMsg = "URL1 Error [";
        errMsg += response.status_code;
        errMsg += "] making request ";
        errMsg += response.text;
        write_errormsg(117, errMsg);
        return true;
    }
    
    Document d;
    if (response.text.length() <= 5) {
        KF_LOG_INFO(logger, "response.text.length <= 5");
        write_errormsg(117, "response.text.length <= 5");
        return true;
    }
    d.Parse(response.text.substr(5).c_str());
    KF_LOG_INFO(logger, "get_quote_requests: " << response.text);
    KF_LOG_INFO(logger, "MDEngineGoogletrends::getGoogleTrends reading req and token from response");

    if (d.IsObject() && d.HasMember("widgets")) {
        rapidjson::Value& reqObj = d["widgets"].GetArray()[0]["request"];
        req = JsonToString(reqObj);
        token = d["widgets"].GetArray()[0]["token"].GetString();
        KF_LOG_INFO(logger, "req:   " << req);
        KF_LOG_INFO(logger, "token: " << token);
        return false;
    }
    else {
        KF_LOG_INFO(logger, "read document with error");
        return true;
    }
}

bool MDEngineGoogletrends::getTimelineData(string url2, vector<string> keyword, string time) {
    KF_LOG_INFO(logger, "MDEngineGoogletrends::getTimelineData");
    cpr::Response response2 = Get(Url{ url2 }
        , cpr::Header{
            {"user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"},
            {"x-client-data", "CJW2yQEIo7bJAQjEtskBCKmdygEIq8fKAQj1x8oBCOnIygEItMvKAQjb1coBCKLWygEIl5rLAQjBmssBGIrBygE="},
            {"cookie", "__utmc=10102256; __utmz=10102256.1604910310.2.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utma=10102256.1148352310.1604907731.1604910829.1605344577.8; CONSENT=YES+CN.zh-CN+202007; SEARCH_SAMESITE=CgQInpAB; __Secure-3PAPISID=Trz6pkCblVViT1ty/AN0A-F-fHN-dql_ik; SSID=AAXMTJ3PjQ2GDlFAJ; SAPISID=Trz6pkCblVViT1ty/AN0A-F-fHN-dql_ik; HSID=AnFtRnCH73Tudlhag; APISID=VNgOW-9vnIvi_EHq/AOr79yEu3Z3kOH1kU; __Secure-3PSID=3Qdi_3ZDiaeH8_g7rbHuW8Y6bztGZ0Kj-Sb26phT9mQ2BtuR8TiasJJckrIvtHyqO1580Q.; SID=3Qdi_3ZDiaeH8_g7rbHuW8Y6bztGZ0Kj-Sb26phT9mQ2BtuRB9LGT3nwjGrpLs6xZlflCw.; ANID=AHWqTUlt2Qcs1kD27OjBq7JuOHjvmUKp0QBtdg5OHRQCOLxeoSQy6wyUUxzEAxSa; 1P_JAR=2020-11-14-08; NID=204=xBa9NggKyoO1aSGJExvSWxTQ9aBYQCEr2qeyU7FGx3WuKJCstPjTRUtgr6P7r2ppVxpGJCW2OdkxHqvRcY47NTKmkPG2gWn-8dNWXXQd6QLRhQI3guqA84zPDEil9tnaNzwF4p2clJmuRse5G1k-v_Fl3RCqXFGqVQfh5ylXCTCJy0oZh31LMra60-MGFZlmVIJrO3Z8HHfIwgZEcv5hPpdP873w1qUibGnXukjJNvXgIy-VFD6eF_uMUzHndiDwAsfgnM7iCEoN_2OZhyelTwQJSwy-kuMUYhcm4GT_4uiuNoho177_GoN5E2wl028_88_MT97qJS3f36DgN_R7pTCbr4bcczfZZo7WPOmjDHVc6PSXvEDV6SyYsKyVE3ecCsg; SIDCC=AJi4QfGlwB1ViYrnkcVmf2JYm-1P1ZXOuL_i1I6asTWLJVFo59acmgo_d99dCYkOi4T4pxfvVfM; __Secure-3PSIDCC=AJi4QfHxaGNTiGPitZYKWjEZM_mgJ6hE_EJ-IzqN6U5PpBEvIhgrVNwp-7ufmvmNA_I1DOhv9Qn-"}
        }, cpr::Timeout{ 1000 });
    if (response2.status_code >= 400) {
        KF_LOG_INFO(logger, "URL2 Error [" << response2.status_code << "] making request ");
        KF_LOG_INFO(logger, "Request took " << response2.elapsed);
        KF_LOG_INFO(logger, "Body: " << response2.text);

        string errMsg;
        errMsg = "URL1 Error [";
        errMsg += response2.status_code;
        errMsg += "] making request ";
        errMsg += response2.text;
        write_errormsg(117, errMsg);
        return true;
    }

    Document d2;
    if (response2.text.length() <= 6) {
        KF_LOG_INFO(logger, "response2.text.length <= 6");
        write_errormsg(117, "response2.text.length <= 6");
        return true;
    }
    d2.Parse(response2.text.substr(6).c_str());
    KF_LOG_INFO(logger, "get_quote_requests: " << response2.text.substr(6));
    KF_LOG_INFO(logger, "MDEngineGoogletrends::getGoogleTrends reading Google Trends");

    int keywordNum = keyword.size();
    if (d2.IsObject() && d2.HasMember("default")) {
        int len = d2["default"]["timelineData"].Size();
        for (int i = 0; i < len; i++) {
            //KF_LOG_INFO(logger, "MDEngineGoogletrends::getGoogleTrends reading timelineData");
            string timeStamp = d2["default"]["timelineData"].GetArray()[i]["time"].GetString();
            string formattedTime = d2["default"]["timelineData"].GetArray()[i]["formattedTime"].GetString();
            string formattedAxisTime = d2["default"]["timelineData"].GetArray()[i]["formattedAxisTime"].GetString();

            //KF_LOG_INFO(logger, "MDEngineGoogletrends::getGoogleTrends timeStamp: " << timeStamp << " formattedTime: " << formattedTime);
            vector<int> valueVec;
            vector<bool> hasDataVec;
            vector<string> formattedValueVec;
            for (int j = 0; j < keywordNum; j++) {
                valueVec.push_back(d2["default"]["timelineData"].GetArray()[i]["value"].GetArray()[j].GetInt());
                hasDataVec.push_back(d2["default"]["timelineData"].GetArray()[i]["hasData"].GetArray()[j].GetBool());
                formattedValueVec.push_back(d2["default"]["timelineData"].GetArray()[i]["formattedValue"].GetArray()[j].GetString());
            }

            //KF_LOG_INFO(logger, "MDEngineGoogletrends::getGoogleTrends writing trends_data ");
            for (int j = 0; j < keywordNum; j++) {
                GoogleTrendsData trend;
                strcpy(trend.KeyWord, keyword[j].c_str());
                strcpy(trend.CountryOrRegion, "global");                //default
                strcpy(trend.Period, time.c_str());
                strcpy(trend.Type, "all");                              //default
                strcpy(trend.GoogleService, "Google for web search");   //default

                trend.Time = atoi(timeStamp.c_str());
                strcpy(trend.FormattedTime, formattedTime.c_str());
                strcpy(trend.FormattedAxisTime, formattedAxisTime.c_str());
                trend.HasData = hasDataVec[j];
                trend.Value = valueVec[j];
                KF_LOG_INFO(logger, "trend.Value:" << trend.Value << " CountryOrRegion:" << trend.CountryOrRegion << " GoogleService:" << trend.GoogleService);
                on_trends_data(&trend);
            }
            valueVec.clear();
            hasDataVec.clear();
            formattedValueVec.clear();
        }
    }
    else {
        KF_LOG_INFO(logger, "read document with error");
        return true;
    }
    return false;
}

void MDEngineGoogletrends::getGoogleTrends(vector<string> keyword, string time) {
    KF_LOG_INFO(logger, "MDEngineGoogletrends::getGoogleTrends");
    if (keyword.size() == 0) {
        KF_LOG_INFO(logger, "keyword.size() == 0, return");
        return;
    }
    else {
        KF_LOG_INFO(logger, "keyword.size() > 0");
        for (auto itr : keyword) {
            KF_LOG_INFO(logger, "keyword: " << itr);
        }
    }
    KF_LOG_INFO(logger, "time: " << time);

    string url = set_token_url(keyword, time);
    url = replace_space(url);
    KF_LOG_INFO(logger, "MDEngineGoogletrends::getGoogleTrends url1: " << url);

    bool retry = true;
    string req;
    string token;
    for (int i = 0; i < 3 && retry; i++) {
        retry = getToken(url, req, token);
        if (retry) {
            KF_LOG_INFO(logger, "retry to getToken");
        }
    }
    if (retry) {
        KF_LOG_INFO(logger, "retry to getToken failed, return");
        return;
    }

    //****************************************************************
    //after getting token and req
    //****************************************************************

    string url2 = set_requset_url(req, token);
    url2 = replace_space(url2);
    KF_LOG_INFO(logger, "url2: " << url2);

    retry = true;
    for (int i = 0; i < 3 && retry; i++) {
        retry = getTimelineData(url2, keyword, time);
        if (retry)
            KF_LOG_INFO(logger, "retry to getTimelineData");
    }
    if (retry) {
        KF_LOG_INFO(logger, "retry to getTimelineData failed, return");
        return;
    }
}

BOOST_PYTHON_MODULE(libgoogletrendsmd)
{
    using namespace boost::python;
    class_<MDEngineGoogletrends, boost::shared_ptr<MDEngineGoogletrends> >("Engine")
        .def(init<>())
        .def("init", &MDEngineGoogletrends::initialize)
        .def("start", &MDEngineGoogletrends::start)
        .def("stop", &MDEngineGoogletrends::stop)
        .def("logout", &MDEngineGoogletrends::logout)
        .def("wait_for_stop", &MDEngineGoogletrends::wait_for_stop);
}

/*
struct GoogleTrendsData
{
    //search param
    char_32		KeyWord;				// bitcoin
    char_32		CountryOrRegion;		// default = global
    char_32		Period;					// timestr
    char_32		Type;					// default = all
    char_32		GoogleService;			// Google for web search

    //response
    int64_t		Time;					// timeStamp
    char_64		FormattedTime;			// timestr
    char_32		FormattedAxisTime;		// timestr
    bool		HasData;				// true or false
    int			Value;					//
};
*/
