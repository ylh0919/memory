#ifndef WINGCHUN_MDENGINEFTX_H
#define WINGCHUN_MDENGINEFTX_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"
#include <document.h>
#include <map>
#include <vector>
#include <websocket.h>
WC_NAMESPACE_START

using rapidjson::Document;

class MDEngineFtx: public IMDEngine,public common::CWebsocketCallBack
{
public:
    /** load internal information from config json */
    virtual void load(const json& j_config);
    virtual void connect(long timeout_nsec);
    virtual void login(long timeout_nsec);
    virtual void logout();
    virtual void release_api();
    virtual void subscribeMarketData(const vector<string>& instruments, const vector<string>& markets);
    virtual bool is_connected() const { return connected; };
    virtual bool is_logged_in() const { return logged_in; };
    virtual string name() const { return "MDEngineFtx"; };

public:
    MDEngineFtx();

    void on_lws_data(struct lws* conn, const char* data, size_t len);
    void on_lws_connection_error(struct lws* conn);

private:
    virtual void OnConnected(const common::CWebsocket* instance);
    virtual void OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg);
    virtual void OnDisconnected(const common::CWebsocket* instance);
    virtual void DoLoopItem();
    inline int64_t getTimestamp();

    void onBook(Document& json);
    void onTrade(Document& json);

    std::string createBookJsonString(std::string exchange_coinpair);
    std::string createTradeJsonString(std::string exchange_coinpair);
    void quote_requests_loop();
    void get_quote_requests();

    virtual void set_reader_thread() override;
    void makeWebsocketSubscribeJsonString();
private:
    ThreadPtr rest_thread;
    ThreadPtr quote_requests_thread;
    bool connected = false;
    bool logged_in = false;

    int book_depth_count = 20;
    int level_threshold = 5;
    int refresh_normal_check_book_s = 120;
    int quote_get_interval_ms = 500;

    int once = 1;

    int64_t timer;

    static constexpr int scale_offset = 1e8;

    PriceBook20Assembler priceBook20Assembler;

    CoinPairWhiteList coinPairWhiteList;

    common::CWebsocket websocket;
    bool is_ws_disconnectd = false;

    std::map<std::string, int64_t> control_book_map;
};

DECLARE_PTR(MDEngineFtx);

WC_NAMESPACE_END

#endif
