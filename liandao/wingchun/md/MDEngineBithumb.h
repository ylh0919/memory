#ifndef WINGCHUN_MDENGINEBITHUMB_H
#define WINGCHUN_MDENGINEBITHUMB_H

#include "IMDEngine.h"
#include "longfist/LFConstants.h"
#include "CoinPairWhiteList.h"
#include "PriceBook20Assembler.h"
#include <libwebsockets.h>
#include <map>
#include <unordered_map>
#include <websocket.h>


WC_NAMESPACE_START

class MDEngineBithumb: public IMDEngine,public common::CWebsocketCallBack
{
public:
	
	enum lws_event
	{
		trade,
		depth5,
		depth20
	};

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
    virtual string name() const { return "MDEngineBithumb"; };

public:
    MDEngineBithumb();
	
	void on_lws_data(struct lws* conn, const char* data, size_t len);
	
	void on_lws_connection_error(struct lws* conn);

private:
    void GetAndHandleDepthResponse(const std::string& symbol, int limit);

    void GetAndHandleTradeResponse(const std::string& symbol, int limit);
	
	void connect_lws(std::string t, lws_event e);
    
	void on_lws_market_trade(const char* data, size_t len);

	void on_lws_book_update(const char* data, size_t len, const std::string& ticker);

    void loop();

    virtual void set_reader_thread() override;
    inline int64_t getTimestamp();
    void makeWebsocketSubscribeJsonString();
    std::string createBookJsonString(std::vector<std::string>& exchange_coinpair);
    std::string createTradeJsonString(std::vector<std::string>& exchange_coinpair);
    void onBook(Document& json);
    void onTrade(Document& json);
    bool getInitPriceBook(const std::string& strSymbol,std::map<std::string,int64_t>::iterator& it);

    virtual void OnConnected(const common::CWebsocket* instance);
    virtual void OnReceivedMessage(const common::CWebsocket* instance,const std::string& msg);
    virtual void OnDisconnected(const common::CWebsocket* instance);

    CoinPairWhiteList coinPairWhiteList;
    PriceBook20Assembler priceBook20Assembler;
    common::CWebsocket websocket;
    bool is_ws_disconnectd = false;

private:
    ThreadPtr rest_thread;
    bool connected = false;
    bool logged_in = false;

    int book_depth_count = 5;
    int trade_count = 10;
    int rest_get_interval_ms = 500;

    uint64_t last_rest_get_ts = 0;
    uint64_t last_trade_id = 0;
    static constexpr int scale_offset = 1e8;
    int64_t timer;
    int level_threshold = 15;
    int refresh_normal_check_book_s = 120;
    int rest_try_count = 1;

    struct lws_context *context = nullptr;
  	
    std::vector<std::string> symbols;
	std::unordered_map<struct lws *,std::pair<std::string, lws_event> > lws_handle_map;
    std::map<std::string,int64_t> m_mapPriceBookData;
};

DECLARE_PTR(MDEngineBithumb);


WC_NAMESPACE_END

#endif
