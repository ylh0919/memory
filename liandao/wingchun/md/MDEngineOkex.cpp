#include "MDEngineOkex.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"
#include "../../utils/common/ld_utils.h"
#include "../../utils/gzip/decompress.hpp"
//#include "../gzip/compress.hpp"

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
#include <zlib.h>

using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;

using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;
using namespace gzip;


USING_WC_NAMESPACE
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;
std::mutex update_mutex;
std::mutex book_mutex;
static MDEngineOkex* global_md = nullptr;

static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{

    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            lws_callback_on_writable( wsi );
            break;
        }
        case LWS_CALLBACK_PROTOCOL_INIT:
        {
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
            if(global_md)
            {
                global_md->on_lws_data(wsi, (const char*)in, len);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_CLOSED:
        {
            if(global_md) {
                std::cout << "3.1415926 LWS_CALLBACK_CLIENT_CLOSED 2,  (call on_lws_connection_error)  reason = " << reason << std::endl;
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE_PONG:
        {
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            if(global_md)
            {
                global_md->lws_write_subscribe(wsi);
            }
            break;
        }
	    case LWS_CALLBACK_TIMER:
        {
            break;
	    }
        case LWS_CALLBACK_CLOSED:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            std::cout << "3.1415926 LWS_CALLBACK_CLOSED/LWS_CALLBACK_CLIENT_CONNECTION_ERROR writeable, reason = " << reason << std::endl;
            if(global_md)
            {
                global_md->on_lws_connection_error(wsi);
            }
            break;
        }
        default:
            break;
    }

    return 0;
}

static struct lws_protocols protocols[] =
        {
                {
                        "md-protocol",
                        ws_service_cb,
                              0,
                                 65536,
                },
                { NULL, NULL, 0, 0 } /* terminator */
        };


enum protocolList {
    PROTOCOL_TEST,

    PROTOCOL_LIST_COUNT
};

struct session_data {
    int fd;
};

MDEngineOkex::MDEngineOkex(): IMDEngine(SOURCE_OKEX)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Okex");
}

void MDEngineOkex::load(const json& j_config)
{
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    priceBook20Assembler.SetLeastLevel(level_threshold);
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    book_depth_count = j_config["book_depth_count"].get<int>();
    priceBook20Assembler.SetLevel(book_depth_count);
    trade_count = j_config["trade_count"].get<int>();
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    KF_LOG_INFO(logger, "MDEngineOkex:: rest_get_interval_ms: " << rest_get_interval_ms);

    if(j_config.find("snapshot_check_s") != j_config.end()) {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }

    //subscribe two kline
    /*if (j_config.find("kline_a_interval") != j_config.end())
        kline_a_interval = j_config["kline_a_interval"].get<string>();
    if (j_config.find("kline_b_interval") != j_config.end())
        kline_b_interval = j_config["kline_b_interval"].get<string>();*/
    if (j_config.find("kline_a_interval_s") != j_config.end())
        kline_a_interval = to_string(j_config["kline_a_interval_s"].get<int64_t>()) + "s";
    if (j_config.find("kline_b_interval_s") != j_config.end())
        kline_b_interval = to_string(j_config["kline_b_interval_s"].get<int64_t>()) + "s";

    coinPairWhiteList.ReadWhiteLists(j_config, "whiteLists");
    coinPairWhiteList.Debug_print();

    makeWebsocketSubscribeJsonString();
    debug_print(websocketSubscribeJsonString);

    //display usage:
    if(coinPairWhiteList.Size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineOkex::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"tBTCUSDT\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"tETCETH\"");
        KF_LOG_ERROR(logger, "},");
    }

    KF_LOG_INFO(logger, "MDEngineOkex::load:  book_depth_count: "
            << book_depth_count << " trade_count: " << trade_count << " rest_get_interval_ms: " << rest_get_interval_ms);
}

void MDEngineOkex::makeWebsocketSubscribeJsonString()
{
	KF_LOG_INFO(logger,"makeWebsocketSubscribeJsonString");
    std::unordered_map<std::string, std::string>::iterator map_itr;
    map_itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin();
    while(map_itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end()) {
        KF_LOG_DEBUG(logger, "[makeWebsocketSubscribeJsonString] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) "<< map_itr->second);

            std::string jsonSnapshotString = createBookJsonString(map_itr->second);
            websocketSubscribeJsonString.push_back(jsonSnapshotString);
            std::string jsonTradeString = createTradeJsonString(map_itr->second);
            websocketSubscribeJsonString.push_back(jsonTradeString);        
            //std::string jsonCandleString = createCandleJsonString(map_itr->second);
            //websocketSubscribeJsonString.push_back(jsonCandleString); 
            if (kline_a_interval != kline_b_interval) {
                std::string jsonCandleString_A = createCandleJsonString(map_itr->second, kline_a_interval);
                std::string jsonCandleString_B = createCandleJsonString(map_itr->second, kline_b_interval);
                websocketSubscribeJsonString.push_back(jsonCandleString_A);
                websocketSubscribeJsonString.push_back(jsonCandleString_B);
            }
            else {
                std::string jsonCandleString = createCandleJsonString(map_itr->second, kline_a_interval);
                websocketSubscribeJsonString.push_back(jsonCandleString);
            }
        
        map_itr++;
    }
    
}

void MDEngineOkex::debug_print(std::vector<std::string> &subJsonString)
{
    size_t count = subJsonString.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeJsonString (subJsonString) " << subJsonString[i]);
    }
}

void MDEngineOkex::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineOkex::connect:");
    connected = true;
}

void MDEngineOkex::login(long timeout_nsec) {
    KF_LOG_INFO(logger, "MDEngineOkex::login:");
    global_md = this;

    if (context == NULL) {
        struct lws_context_creation_info info;
        memset( &info, 0, sizeof(info) );

        info.port = CONTEXT_PORT_NO_LISTEN;
        info.protocols = protocols;
        info.iface = NULL;
        info.ssl_cert_filepath = NULL;
        info.ssl_private_key_filepath = NULL;
        info.extensions = NULL;
        info.gid = -1;
        info.uid = -1;
        info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
        info.max_http_header_pool = 1024;
        info.fd_limit_per_thread = 1024;
        info.ws_ping_pong_interval = 10;
        info.ka_time = 10;
        info.ka_probes = 10;
        info.ka_interval = 10;

        context = lws_create_context( &info );
        KF_LOG_INFO(logger, "MDEngineOkex::login: context created.");
    }

    if (context == NULL) {
        KF_LOG_ERROR(logger, "MDEngineOkex::login: context is NULL. return");
        return;
    }

    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;
    lws_set_log_level(logs, NULL);

    struct lws_client_connect_info ccinfo = {0};

    static std::string host  = "real.OKEx.com";
    static std::string path = "/ws/v3";
    static int port = 8443;

    ccinfo.context 	= context;
    ccinfo.address 	= host.c_str();
    ccinfo.port 	= port;
    ccinfo.path 	= path.c_str();
    ccinfo.host 	= host.c_str();
    ccinfo.origin 	= host.c_str();
    ccinfo.ietf_version_or_minus_one = -1;
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;

    struct lws* wsi = lws_client_connect_via_info(&ccinfo);
    KF_LOG_INFO(logger, "MDEngineOkex::login: Connecting to " <<  ccinfo.host << ":" << ccinfo.port << ":" << ccinfo.path);

    if (wsi == NULL) {
        KF_LOG_ERROR(logger, "MDEngineOkex::login: wsi create error.");
        return;
    }
    KF_LOG_INFO(logger, "MDEngineOkex::login: wsi create success.");

    logged_in = true;
    timer = getTimestamp();
}

void MDEngineOkex::set_reader_thread()
{
    IMDEngine::set_reader_thread();

    //rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineOkex::rest_loop, this)));
    ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineOkex::loop, this)));
    //check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineOkex::check_loop, this)));
}

void MDEngineOkex::logout()
{
    KF_LOG_INFO(logger, "MDEngineOkex::logout:");
}

void MDEngineOkex::release_api()
{
    KF_LOG_INFO(logger, "MDEngineOkex::release_api:");
}

void MDEngineOkex::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
    KF_LOG_INFO(logger, "MDEngineOkex::subscribeMarketData:");
}

int MDEngineOkex::lws_write_subscribe(struct lws* conn)
{
    KF_LOG_INFO(logger, "MDEngineOkex::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    //有待发送的数据，先把待发送的发完，在继续订阅逻辑。  ping?
    if(websocketPendingSendMsg.size() > 0) {
        unsigned char msg[512];
        memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

        std::string jsonString = websocketPendingSendMsg.back();
        websocketPendingSendMsg.pop_back();
        KF_LOG_INFO(logger, "MDEngineOkex::lws_write_subscribe: websocketPendingSendMsg" << jsonString.c_str());
        int length = jsonString.length();

        strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

        if(websocketPendingSendMsg.size() > 0)
        {    //still has pending send data, emit a lws_callback_on_writable()
            lws_callback_on_writable( conn );
            KF_LOG_INFO(logger, "MDEngineOkex::lws_write_subscribe: (websocketPendingSendMsg,size)" << websocketPendingSendMsg.size());
        }
        return ret;
    }

    if(websocketSubscribeJsonString.size() == 0) return 0;
    //sub depth
    if(subscribe_index >= websocketSubscribeJsonString.size())
    {
        //subscribe_index = 0;
        KF_LOG_INFO(logger, "MDEngineOkex::lws_write_subscribe: (none reset subscribe_index = 0, just return 0)");
	    return 0;
    }

    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);

    std::string jsonString = websocketSubscribeJsonString[subscribe_index++];

    KF_LOG_INFO(logger, "MDEngineOkex::lws_write_subscribe: " << jsonString.c_str() << " ,after ++, (subscribe_index)" << subscribe_index);
    int length = jsonString.length();

    strncpy((char *)msg+LWS_PRE, jsonString.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);

    if(subscribe_index < websocketSubscribeJsonString.size())
    {
        lws_callback_on_writable( conn );
        KF_LOG_INFO(logger, "MDEngineOkex::lws_write_subscribe: (subscribe_index < websocketSubscribeJsonString) call lws_callback_on_writable");
    }

    return ret;
}
int MDEngineOkex::gzDecompress(const char *src, int srcLen, const char *dst, int dstLen){
    int err = 0;
    z_stream d_stream = {0}; /* decompression stream */

    static char dummy_head[2] = {
            0x8 + 0x7 * 0x10,
            (((0x8 + 0x7 * 0x10) * 0x100 + 30) / 31 * 31) & 0xFF,
    };

    d_stream.zalloc = NULL;
    d_stream.zfree = NULL;
    d_stream.opaque = NULL;
    d_stream.next_in = (Byte *)src;
    d_stream.avail_in = 0;
    d_stream.next_out = (Byte *)dst;


    if (inflateInit2(&d_stream, -MAX_WBITS) != Z_OK) {
        return -1;
    }

    // if(inflateInit2(&d_stream, 47) != Z_OK) return -1;

    while (d_stream.total_out < dstLen && d_stream.total_in < srcLen) {
        d_stream.avail_in = d_stream.avail_out = 1; /* force small buffers */
        if((err = inflate(&d_stream, Z_NO_FLUSH)) == Z_STREAM_END)
            break;

        if (err != Z_OK) {
            if (err == Z_DATA_ERROR) {
                d_stream.next_in = (Bytef*) dummy_head;
                d_stream.avail_in = sizeof(dummy_head);
                if((err = inflate(&d_stream, Z_NO_FLUSH)) != Z_OK) {
                    return -1;
                }
            } else {
                return -1;
            }
        }
    }

    if (inflateEnd(&d_stream)!= Z_OK)
        return -1;
    dstLen = d_stream.total_out;
    return 0;
}
void MDEngineOkex::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    KF_LOG_INFO(logger, "MDEngineOkex::on_lws_data:");
    int l = len*10;
    char* buf = new char[l]{};
    int result = gzDecompress(data, len, buf, l);
    KF_LOG_INFO(logger, "[on_lws_data]: "<< buf);
    
    Document json;
    json.Parse(buf);

    if(json.HasParseError()) {
        KF_LOG_ERROR(logger, "MDEngineOkex::on_lws_data. parse json error: " << buf);
        std::string new_buf = string(buf) + "}";
        json.Parse(new_buf.c_str());
        if(json.HasParseError()){
        	KF_LOG_INFO(logger,"will return");
            delete[] buf;
        	return;
        }
    }

    if(json.HasMember("table")){
	    string table = json["table"].GetString();

	    if (table == "spot/trade") {
	        onTrade(json);
	    } 
	    else if (table == "spot/depth_l2_tbt") 
	    {
	        onBook(json);
	    } 
	    //else if(table == "spot/candle60s") {
	    else if(table.find("spot/candle") != -1) {
	        onCandle(json);               
	    }
	    else {
	        KF_LOG_INFO(logger, "MDEngineOkex::on_lws_data: unknown array data: " << data);
	    }
	}
    delete[] buf;
}


void MDEngineOkex::on_lws_connection_error(struct lws* conn)
{
    KF_LOG_ERROR(logger, "MDEngineOkex::on_lws_connection_error.");
    //market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineOkex::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
    priceBook20Assembler.clearPriceBook();
    //no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}


int64_t MDEngineOkex::getTimestamp()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}


void MDEngineOkex::debug_print(std::vector<SubscribeChannel> &websocketSubscribeChannel)
{
    size_t count = websocketSubscribeChannel.size();
    KF_LOG_INFO(logger, "[debug_print] websocketSubscribeChannel (count) " << count);

    for (size_t i = 0; i < count; i++)
    {
        KF_LOG_INFO(logger, "[debug_print] websocketSubscribeChannel (subType) "
                            << websocketSubscribeChannel[i].subType <<
                            " (channelname)" << websocketSubscribeChannel[i].channelname);
    }
}


void MDEngineOkex::onTrade(Document& json)
{
    KF_LOG_INFO(logger, "MDEngineOkex::onTrade");

    std::string ticker;
    Value &node = json["data"];
    ticker = node.GetArray()[0]["instrument_id"].GetString();
    ticker = coinPairWhiteList.GetKeyByValue(ticker);
    KF_LOG_INFO(logger,"ticker="<<ticker);
    if(ticker.empty())
    {
    	KF_LOG_INFO(logger,"ticker.empty()");
        return;
    }

    LFL2TradeField trade;
    memset(&trade, 0, sizeof(trade));
    strcpy(trade.InstrumentID, ticker.c_str());
    strcpy(trade.ExchangeID, "okex");

    trade.Price = std::round(stod(node.GetArray()[0]["price"].GetString()) * scale_offset);
    trade.Volume = std::round(stod(node.GetArray()[0]["size"].GetString()) * scale_offset);
    std::string side = node.GetArray()[0]["side"].GetString();
    trade.OrderBSFlag[0] = side == "buy" ? 'B' : 'S';    
    
    std::string tridetime = node.GetArray()[0]["timestamp"].GetString();
    strcpy(trade.TradeTime, tridetime.c_str());  
    trade.TimeStamp = formatISO8601_to_timestamp(tridetime)*1000000;

    std::string tradeid = node.GetArray()[0]["trade_id"].GetString();
    strcpy(trade.TradeID, tradeid.c_str());

    KF_LOG_INFO(logger, "MDEngineOkex::[onTrade] (ticker)" << ticker <<
                                                                   " (Price)" << trade.Price <<
                                                                   " (trade.Volume)" << trade.Volume <<
                                                                   " (trade.TradeID)" << trade.TradeID <<
                                                                   " (trade.TimeStamp)" << trade.TimeStamp);
                                                                
                                                               
    on_trade(&trade);
        
    
}

void MDEngineOkex::onBook(Document& json)
{
    KF_LOG_INFO(logger, "MDEngineOkex::onBook");

    Value &data = json["data"];
    std::string exchange_coinpair = data.GetArray()[0]["instrument_id"].GetString();
    std::string ticker = coinPairWhiteList.GetKeyByValue(exchange_coinpair);  
    std::string sequence = data.GetArray()[0]["timestamp"].GetString();
    
    if(ticker.empty()){
        KF_LOG_INFO(logger,"no ticker");
        return;
    }
    
    std::string action = json["action"].GetString();
    if(action == "partial")//snapshot 
    {
        priceBook20Assembler.clearPriceBook(ticker);
        auto& bids = data.GetArray()[0]["bids"];  
        int len = bids.Size(); 
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(stod(bids[i][0].GetString()) * scale_offset);
            uint64_t volume = std::round(stod(bids[i][1].GetString()) * scale_offset);
            priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
        }
        auto& asks = data.GetArray()[0]["asks"]; 
        len = asks.Size();  
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(stod(asks[i][0].GetString()) * scale_offset);
            uint64_t volume = std::round(stod(asks[i][1].GetString()) * scale_offset);
            priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
        }
    }
    else{
    	//KF_LOG_INFO(logger,"update=");
        auto& bids = data.GetArray()[0]["bids"];  
        int len = bids.Size(); 
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(stod(bids[i][0].GetString()) * scale_offset);
            uint64_t volume = std::round(stod(bids[i][1].GetString()) * scale_offset);
            //KF_LOG_INFO(logger,"bids: price="<<price<<" volume="<<volume);
            if(volume == 0){
            	//KF_LOG_INFO(logger,"erase");
            	priceBook20Assembler.EraseBidPrice(ticker, price);
            }else{
            	//KF_LOG_INFO(logger,"update");
            	priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
            }
        }
        auto& asks = data.GetArray()[0]["asks"]; 
        len = asks.Size();  
        for (int i = 0; i < len; i++) {
            int64_t price = std::round(stod(asks[i][0].GetString()) * scale_offset);
            uint64_t volume = std::round(stod(asks[i][1].GetString()) * scale_offset);
            //KF_LOG_INFO(logger,"asks: price="<<price<<" volume="<<volume);
            if(volume == 0){
            	//KF_LOG_INFO(logger,"erase");
            	priceBook20Assembler.EraseAskPrice(ticker, price);
            }else{
            	//KF_LOG_INFO(logger,"update");
            	priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
            }
        }    
    }
    
    // has any update
    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    if(priceBook20Assembler.Assembler(ticker, md)) {
        timer = getTimestamp();
        if(md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold)
        {
            KF_LOG_INFO(logger,"failed ,level count < level threshold");
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"bids["<<i<<"]:"<<md.BidLevels[i].price);
            }
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"asks["<<i<<"]:"<<md.AskLevels[i].price);
            }
            string errorMsg = "orderbook level below threshold";
            md.Status = 1;
            write_errormsg(112,errorMsg);
        }
        else if(md.BidLevels[0].price > md.AskLevels[0].price)
        {
            KF_LOG_INFO(logger,"failed ,orderbook crossed");
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"bids["<<i<<"]:"<<md.BidLevels[i].price);
            }
            for(int i=0;i<20;i++){
                KF_LOG_INFO(logger,"asks["<<i<<"]:"<<md.AskLevels[i].price);
            }
            string errorMsg = "orderbook crossed";
            md.Status = 2;
            write_errormsg(113,errorMsg);
        }
        else
        {
            strcpy(md.ExchangeID, "okex");
            /*
            BookMsg bookmsg;
            bookmsg.book = md;
            bookmsg.time = getTimestamp();
            bookmsg.sequence = sequence;
            std::unique_lock<std::mutex> lck1(ws_book_mutex);
            auto itr = ws_book_map.find(ticker);
            if(itr == ws_book_map.end()){
                std::vector<BookMsg> bookmsg_vec;
                bookmsg_vec.push_back(bookmsg);                
                ws_book_map.insert(make_pair(ticker, bookmsg_vec));
                KF_LOG_INFO(logger,"insert:"<<ticker);
            }else{
                itr->second.push_back(bookmsg);
            }            
            lck1.unlock();

            std::unique_lock<std::mutex> lck4(book_mutex);
            book_map[ticker] = bookmsg;
            lck4.unlock();            
            */
            KF_LOG_INFO(logger,"ws sequence="<<md.UpdateMicroSecond);
            on_price_book_update(&md);
        }
    }
    else
    {
        /*
            std::unique_lock<std::mutex> lck4(book_mutex);
            auto it = book_map.find(ticker);
            if(it != book_map.end()){
                BookMsg bookmsg;
                bookmsg.book = it->second.book;
                bookmsg.time = getTimestamp();
                bookmsg.sequence = sequence;
                std::unique_lock<std::mutex> lck1(ws_book_mutex);
                auto itr = ws_book_map.find(ticker);
                if(itr != ws_book_map.end()){
                    itr->second.push_back(bookmsg);
                }
                lck1.unlock(); 

            }
            lck4.unlock();
        */
    }
}

void MDEngineOkex::onCandle(Document& json)
{
	KF_LOG_INFO(logger, "MDEngineOkex::onCandle");	

     auto& candle = json["data"].GetArray()[0]["candle"];
     std::string instrument = json["data"].GetArray()[0]["instrument_id"].GetString();
     instrument = coinPairWhiteList.GetKeyByValue(instrument);

     auto& tickKLine = mapKLines[instrument];
     LFBarMarketDataField market;
     memset(&market, 0, sizeof(market));
     strcpy(market.InstrumentID, instrument.c_str());
     strcpy(market.ExchangeID, "okex");

     time_t now = time(0);
     struct tm tradeday = *localtime(&now);
     strftime(market.TradingDay, 9, "%Y%m%d", &tradeday);

     //"spot/candle60s" -> 60
     string table = json["table"].GetString();
     int interval = stod(table.substr(11, table.length() - 12));

     std::string str_starttime = candle.GetArray()[0].GetString();
     int64_t nStartTime = formatISO8601_to_timestamp(str_starttime);
     KF_LOG_INFO(logger,"nStartTime="<<nStartTime);
     nStartTime /= 1e3;
     //int64_t nEndTime = nStartTime + 59;
     int64_t nEndTime = nStartTime + interval - 1;
     market.StartUpdateMillisec = nStartTime*1000;
     struct tm start_tm = *localtime((time_t*)(&nStartTime));
     sprintf(market.StartUpdateTime,"%02d:%02d:%02d.000", start_tm.tm_hour,start_tm.tm_min,start_tm.tm_sec);

     //market.EndUpdateMillisec = market.StartUpdateMillisec+59999;
     market.EndUpdateMillisec = market.StartUpdateMillisec + interval * 1000 - 1;
     struct tm end_tm = *localtime((time_t*)(&nEndTime));
     sprintf(market.EndUpdateTime,"%02d:%02d:%02d.999", end_tm.tm_hour,end_tm.tm_min,end_tm.tm_sec);
     //market.PeriodMillisec = 60000;
     market.PeriodMillisec = interval * 1000;

     market.Open = std::round(stod(candle.GetArray()[1].GetString()) * scale_offset);
     market.Close = std::round(stod(candle.GetArray()[4].GetString()) * scale_offset);
     market.Low = std::round(stod(candle.GetArray()[3].GetString()) * scale_offset);
     market.High = std::round(stod(candle.GetArray()[2].GetString()) * scale_offset);      
     market.Volume = std::round(stod(candle.GetArray()[5].GetString()) * scale_offset);
     if(candle.Size() > 6)
        market.CurrencyVolume = std::round(stod(candle.GetArray()[6].GetString()) * scale_offset);
     auto it = tickKLine.find(nStartTime);
     if(tickKLine.size() == 0 || it != tickKLine.end())
     {
        tickKLine[nStartTime]=market;
        KF_LOG_INFO(logger, "doKlineData(cached): StartUpdateMillisec "<< market.StartUpdateMillisec << " StartUpdateTime "<<market.StartUpdateTime << " EndUpdateMillisec "<< market.EndUpdateMillisec << " EndUpdateTime "<<market.EndUpdateTime
            << "Open" << market.Open << " Close " <<market.Close << " Low " <<market.Low << " Volume " <<market.Volume); 
     }
     else
     {
        on_market_bar_data(&(tickKLine.begin()->second));
        tickKLine.clear();
        tickKLine[nStartTime]=market;
        KF_LOG_INFO(logger, "upKlineData: StartUpdateMillisec "<< market.StartUpdateMillisec << " StartUpdateTime "<<market.StartUpdateTime << " EndUpdateMillisec "<< market.EndUpdateMillisec << " EndUpdateTime "<<market.EndUpdateTime
            << "Open" << market.Open << " Close " <<market.Close << " Low " <<market.Low << " Volume " <<market.Volume);
     }
}

std::string MDEngineOkex::parseJsonToString(Document &d)
{
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);

    return buffer.GetString();
}


//{"op": "subscribe", "args": ["spot/depth_l2_tbt:ETH-USDT"]}
std::string MDEngineOkex::createBookJsonString(std::string exchange_coinpair)
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("op");
	writer.String("subscribe");

	writer.Key("args");
	writer.StartArray();
	std::string trade_ticker = "spot/depth_l2_tbt:" + exchange_coinpair;
	writer.String(trade_ticker.c_str());
	writer.EndArray();

	writer.EndObject();
	return s.GetString();
}

//{"op": "subscribe", "args": ["spot/trade:ETH-USDT"]}
std::string MDEngineOkex::createTradeJsonString(std::string exchange_coinpair)
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("op");
	writer.String("subscribe");

	writer.Key("args");
	writer.StartArray();
	std::string trade_ticker = "spot/trade:" + exchange_coinpair;
	writer.String(trade_ticker.c_str());
	writer.EndArray();

	writer.EndObject();
	return s.GetString();
}

//{"op": "subscribe", "args": ["spot/candle60s:ETH-USDT"]}
std::string MDEngineOkex::createCandleJsonString(std::string exchange_coinpair)
{
	StringBuffer s;
	Writer<StringBuffer> writer(s);
	writer.StartObject();
	writer.Key("op");
	writer.String("subscribe");

	writer.Key("args");
	writer.StartArray();
	std::string trade_ticker = "spot/candle60s:" + exchange_coinpair;
	writer.String(trade_ticker.c_str());
	writer.EndArray();

	writer.EndObject();
	return s.GetString();
}

std::string MDEngineOkex::createCandleJsonString(std::string exchange_coinpair, std::string interval)
{
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    writer.StartObject();
    writer.Key("op");
    writer.String("subscribe");

    writer.Key("args");
    writer.StartArray();
    std::string trade_ticker = "spot/candle" + interval + ":" + exchange_coinpair;
    writer.String(trade_ticker.c_str());
    writer.EndArray();

    writer.EndObject();
    return s.GetString();
}

void MDEngineOkex::get_snapshot_via_rest()
{
    {
        for(auto itr = coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().begin(); itr != coinPairWhiteList.GetKeyIsStrategyCoinpairWhiteList().end(); itr++)
        {
            std::string url = "https://www.okex.com/api/spot/v3/instruments/";
            url += itr->second;
            url += "/book?size=20";
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            //"code":"200000"
            if(d.IsObject() && d.HasMember("asks") && d.HasMember("bids"))
            {
                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "okex");
                strncpy(priceBook.InstrumentID, itr->first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, itr->first.size()));
                if(d.HasMember("bids") && d["bids"].IsArray())
                {
                    auto& bids = d["bids"];
                    int len = std::min((int)bids.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.BidLevels[i].price = std::round(stod(bids[i][0].GetString()) * scale_offset);
                        priceBook.BidLevels[i].volume = std::round(stod(bids[i][1].GetString()) * scale_offset);
                    }
                    priceBook.BidLevelCount = len;
                }
                if (d.HasMember("asks") && d["asks"].IsArray())
                {
                    auto& asks = d["asks"];
                    int len = std::min((int)asks.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.AskLevels[i].price = std::round(stod(asks[i][0].GetString()) * scale_offset);
                        priceBook.AskLevels[i].volume = std::round(stod(asks[i][1].GetString()) * scale_offset);
                    }
                    priceBook.AskLevelCount = len;
                }
                std::string sequence = d["timestamp"].GetString();

                BookMsg bookmsg;
                bookmsg.time = getTimestamp();
                bookmsg.book = priceBook;
                bookmsg.sequence = sequence;
                std::unique_lock<std::mutex> lck3(rest_book_mutex);
                rest_book_vec.push_back(bookmsg);    
                lck3.unlock();           

                on_price_book_update_from_rest(&priceBook);
            }
        }
    }
    

}

void MDEngineOkex::check_snapshot()
{
    std::vector<BookMsg>::iterator rest_it;
    std::unique_lock<std::mutex> lck3(rest_book_mutex);
    for(rest_it = rest_book_vec.begin();rest_it != rest_book_vec.end();){
        bool has_same_book = false;
        int64_t now = getTimestamp();
        //bool has_error = false;
        //KF_LOG_INFO(logger,"string(rest_it->book.InstrumentID)"<<string(rest_it->book.InstrumentID));
        std::unique_lock<std::mutex> lck1(ws_book_mutex);
        auto map_itr = ws_book_map.find(string(rest_it->book.InstrumentID));
        if(map_itr != ws_book_map.end()){
            std::vector<BookMsg>::iterator ws_it;
            for(ws_it = map_itr->second.begin(); ws_it != map_itr->second.end();){
                //KF_LOG_INFO(logger,"sequence_1="<<ws_it->sequence<<" sequence_2="<<rest_it->sequence);
                if(ws_it->sequence < rest_it->sequence){
                    KF_LOG_INFO(logger,"erase old");
                    ws_it = map_itr->second.erase(ws_it);
                    continue;
                }
                else if(ws_it->sequence == rest_it->sequence){
                    bool same_book = true;
                    for(int i = 0; i < 20; i++ ){
                        if(ws_it->book.BidLevels[i].price != rest_it->book.BidLevels[i].price || ws_it->book.BidLevels[i].volume != rest_it->book.BidLevels[i].volume || 
                           ws_it->book.AskLevels[i].price != rest_it->book.AskLevels[i].price || ws_it->book.AskLevels[i].volume != rest_it->book.AskLevels[i].volume)
                        {
                            same_book = false;
                            //has_error = true;
                            KF_LOG_INFO(logger, "2ws snapshot is not same as rest snapshot.sequence = "<< rest_it->sequence);
                            KF_LOG_INFO(logger,"ws_it:"<<ws_it->book.BidLevels[i].price<<" "<<ws_it->book.BidLevels[i].volume<<
                                " "<<ws_it->book.AskLevels[i].price<<" "<<ws_it->book.AskLevels[i].volume);
                            KF_LOG_INFO(logger,"rest_it:"<<rest_it->book.BidLevels[i].price<<" "<<rest_it->book.BidLevels[i].volume<<
                                " "<<rest_it->book.AskLevels[i].price<<" "<<rest_it->book.AskLevels[i].volume);
                            string errorMsg = "ws snapshot is not same as rest snapshot";
                            write_errormsg(116,errorMsg);                           
                            break;
                        }
                    }
                    if(same_book)
                    {
                        has_same_book = true;
                        if(ws_it->time - rest_it->time > snapshot_check_s * 1000){
                            KF_LOG_INFO(logger, "ws snapshot is later than rest snapshot");
                            //rest_it = rest_book_vec.erase(rest_it);
                            string errorMsg = "ws snapshot is later than rest snapshot";
                            write_errormsg(115,errorMsg);
                        }
                        KF_LOG_INFO(logger, "same_book:"<<rest_it->book.InstrumentID);
                        KF_LOG_INFO(logger,"ws_time="<<ws_it->time<<" rest_time="<<rest_it->time);
                        //break;
                    }
                    break;
                }
                else{
                    ws_it++;
                }
            }
        }
        lck1.unlock();
        rest_it = rest_book_vec.erase(rest_it);
    }
    lck3.unlock();
}

int64_t last_rest_time = 0;
void MDEngineOkex::rest_loop()
{
        while(isRunning)
        {
            int64_t now = getTimestamp();
            if((now - last_rest_time) >= rest_get_interval_ms)
            {
                last_rest_time = now;
                get_snapshot_via_rest();
            }
        }
}
int64_t last_check_time = 0;
void MDEngineOkex::check_loop()
{
        while(isRunning)
        {
            int64_t now = getTimestamp();
            if((now - last_check_time) >= rest_get_interval_ms)
            {
                last_check_time = now;
                check_snapshot();
            }
        }
}

void MDEngineOkex::loop()
{
    while(isRunning)
    {
        int64_t now = getTimestamp();
        if ((now - timer) > refresh_normal_check_book_s * 1000)
        {
            KF_LOG_INFO(logger, "failed price book update");
            write_errormsg(114,"orderbook max refresh wait time exceeded");
            timer = now;
        }
        int n = lws_service( context, rest_get_interval_ms );
        //std::cout << " 3.1415 loop() lws_service (n)" << n << std::endl;
    }
}

BOOST_PYTHON_MODULE(libokexmd)
{
    using namespace boost::python;
    class_<MDEngineOkex, boost::shared_ptr<MDEngineOkex> >("Engine")
            .def(init<>())
            .def("init", &MDEngineOkex::initialize)
            .def("start", &MDEngineOkex::start)
            .def("stop", &MDEngineOkex::stop)
            .def("logout", &MDEngineOkex::logout)
            .def("wait_for_stop", &MDEngineOkex::wait_for_stop);
}


