#include "MDEngineKumex.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"

#include <writer.h>
#include <ctype.h>
#include <string.h>
#include <stringbuffer.h>
#include <document.h>
#include <cctype>
#include <algorithm>
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <assert.h>
#include <string>
#include <cpr/cpr.h>
#include <chrono>
#include <utility>
#include <atomic>
#include "../../utils/crypto/openssl_util.h"


using cpr::Get;
using cpr::Url;
using cpr::Parameters;
using cpr::Payload;
using cpr::Post;
using cpr::Body;
using cpr::Timeout;

using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::Value;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using utils::crypto::base64_encode;
using std::string;
using std::to_string;
using std::stod;
using std::stoi;
using namespace std;



USING_WC_NAMESPACE
std::mutex ws_book_mutex;
std::mutex rest_book_mutex;
std::mutex update_mutex;
std::mutex kline_mutex;
std::mutex book_mutex;

static MDEngineKumex* global_md = nullptr;
//std::vector<std::string> symbol_name;
std::map<std::string,std::string> rootSymbol_name;

static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{
    std::stringstream ss;
    ss << "lws_callback,reason=" << reason << ",";
	switch( reason )
	{
		case LWS_CALLBACK_CLIENT_ESTABLISHED:
		{
            ss << "LWS_CALLBACK_CLIENT_ESTABLISHED.";
            global_md->writeErrorLog(ss.str());
			//lws_callback_on_writable( wsi );
			break;
		}
		case LWS_CALLBACK_PROTOCOL_INIT:
        {
			 ss << "LWS_CALLBACK_PROTOCOL_INIT.";
            global_md->writeErrorLog(ss.str());
			break;
		}
		case LWS_CALLBACK_CLIENT_RECEIVE:
		{
		     ss << "LWS_CALLBACK_CLIENT_RECEIVE.";
            global_md->writeErrorLog(ss.str());
			if(global_md)
			{
				global_md->on_lws_data(wsi, (const char*)in, len);
			}
			break;
		}
		case LWS_CALLBACK_CLIENT_WRITEABLE:
		{
		    ss << "LWS_CALLBACK_CLIENT_WRITEABLE.";
            global_md->writeErrorLog(ss.str());
            int ret = 0;
			if(global_md)
			{
				ret = global_md->lws_write_subscribe(wsi);
			}
			break;
		}
		case LWS_CALLBACK_CLOSED:
        {
           // ss << "LWS_CALLBACK_CLOSED.";
           // global_md->writeErrorLog(ss.str());
           // break;
        }
        case LWS_CALLBACK_WSI_DESTROY:
		case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
		{
           // ss << "LWS_CALLBACK_CLIENT_CONNECTION_ERROR.";
            global_md->writeErrorLog(ss.str());
 			if(global_md)
			{
				global_md->on_lws_connection_error(wsi);
			}
			break;
		}
		default:
              global_md->writeErrorLog(ss.str());
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

MDEngineKumex::MDEngineKumex(): IMDEngine(SOURCE_KUCOIN)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Kumex");
    m_mutexPriceBookData = new std::mutex;
    timer = getTimestamp();/*edited by zyy*/
}

MDEngineKumex::~MDEngineKumex()
{
   if(m_mutexPriceBookData)
   {
       delete m_mutexPriceBookData;
   }
}

void MDEngineKumex::writeErrorLog(std::string strError)
{
    KF_LOG_ERROR(logger, strError);
}

void MDEngineKumex::load(const json& j_config)
{
    KF_LOG_ERROR(logger, "MDEngineKumex::load:");
    /*edited by zyy,starts here*/
    if(j_config.find("level_threshold") != j_config.end()) {
        level_threshold = j_config["level_threshold"].get<int>();
    }
    if(j_config.find("refresh_normal_check_book_s") != j_config.end()) {
        refresh_normal_check_book_s = j_config["refresh_normal_check_book_s"].get<int>();
    }
    /*edited by zyy ends here*/
    if(j_config.find("snapshot_check_s") != j_config.end()) {
        snapshot_check_s = j_config["snapshot_check_s"].get<int>();
    }
    rest_get_interval_ms = j_config["rest_get_interval_ms"].get<int>();
    book_depth_count = j_config["book_depth_count"].get<int>();
    //fix for book_depth_count
    book_depth_count = std::min(book_depth_count , 20);
    priceBook20Assembler.SetLevel(book_depth_count);
    if (j_config.find("baseUrl") != j_config.end())
        baseUrl = j_config["baseUrl"].get<string>();

    rest_try_count = j_config["rest_try_count"].get<int>();
    readWhiteLists(j_config);

    debug_print(keyIsStrategyCoinpairWhiteList);
    //display usage:
    if(keyIsStrategyCoinpairWhiteList.size() == 0) {
        KF_LOG_ERROR(logger, "MDEngineKumex::lws_write_subscribe: subscribeCoinBaseQuote is empty. please add whiteLists in kungfu.json like this :");
        KF_LOG_ERROR(logger, "\"whiteLists\":{");
        KF_LOG_ERROR(logger, "    \"strategy_coinpair(base_quote)\": \"exchange_coinpair\",");
        KF_LOG_ERROR(logger, "    \"btc_usdt\": \"btcusdt\",");
        KF_LOG_ERROR(logger, "     \"etc_eth\": \"etceth\"");
        KF_LOG_ERROR(logger, "},");
    }

    int64_t nowTime = getTimestamp();
    std::map<std::string, std::string>::iterator it;
    for(it = keyIsStrategyCoinpairWhiteList.begin(); it != keyIsStrategyCoinpairWhiteList.end(); it++)
    {
        std::unique_lock<std::mutex> lck(book_mutex);
        control_book_map.insert(make_pair(it->first, nowTime));
        lck.unlock();
    }
    
}

void MDEngineKumex::readWhiteLists(const json& j_config)
{
	KF_LOG_INFO(logger, "[readWhiteLists]");
    //string url1="https://api.kumex.com/api/v1/contracts/active";
    string url1 = baseUrl + "/api/v1/contracts/active";
    const auto response = Get(Url{url1});
    Document d;
    d.Parse(response.text.c_str());
    if(!d.HasParseError())
    {
        KF_LOG_INFO(logger,"d.Parse run");
        if(d.IsObject())
        for(int i=0;i<d["data"].Size();i++)
        {
                rootSymbol_name.insert(std::make_pair(d["data"].GetArray()[i]["symbol"].GetString(),d["data"].GetArray()[i]["rootSymbol"].GetString()));
                //symbol_name.push_back(d["data"].GetArray()[i]["symbol"].GetString());
                //rootSymbol_name.push_back(d["data"].GetArray()[i]["rootSymbol"].GetString());
                KF_LOG_DEBUG(logger, "symbol: " << d["data"].GetArray()[i]["symbol"].GetString() << ", rootSymbol: " << d["data"].GetArray()[i]["rootSymbol"].GetString());
        }
    }
    if(j_config.find("whiteLists") != j_config.end()) {
		KF_LOG_INFO(logger, "[readWhiteLists] found whiteLists");
		//has whiteLists
		json whiteLists = j_config["whiteLists"].get<json>();
		if(whiteLists.is_object())
		{
			for (json::iterator it = whiteLists.begin(); it != whiteLists.end(); ++it) 
            {
                    std::string strategy_coinpair = it.key();
                    std::string exchange_coinpair = it.value();
                    if(strategy_coinpair.find("_futures")!=-1){
                        KF_LOG_INFO(logger,"findthe_futures:"<<strategy_coinpair);
                        for(auto rootSymbol_str:rootSymbol_name)
                        {
                            std::string rootvalue=rootSymbol_str.second;
                            std::string symbolvalue=rootSymbol_str.first;
                            if(exchange_coinpair==rootvalue)
                            {
                                m_vstrSubscribeJsonString.push_back(makeSubscribeL2Update(symbolvalue));
                                m_vstrSubscribeJsonString.push_back(makeSubscribeMatch(symbolvalue));
                                m_vstrSubscribeJsonString.push_back(makeSubscribeFunding(symbolvalue));
                                KF_LOG_INFO(logger, "[readWhiteLists] (strategy_coinpair) " << strategy_coinpair << " (exchange_coinpair) " << symbolvalue);
                                    //keyIsStrategyCoinpairWhiteList.insert(std::pair<std::string, std::string>(strategy_coinpair, symbolvalue));
                                    //break;
                            }
                        }
                    }
                    else{
                        m_vstrSubscribeJsonString.push_back(makeSubscribeL2Update(exchange_coinpair));
                        m_vstrSubscribeJsonString.push_back(makeSubscribeMatch(exchange_coinpair));
                        m_vstrSubscribeJsonString.push_back(makeSubscribeFunding(exchange_coinpair));
                        KF_LOG_INFO(logger, "[readWhiteLists] (strategy_coinpair) " << strategy_coinpair << " (exchange_coinpair) " << exchange_coinpair);
                        keyIsStrategyCoinpairWhiteList.insert(std::pair<std::string, std::string>(strategy_coinpair, exchange_coinpair));
                    }
				    //KF_LOG_INFO(logger, "[readWhiteLists] (strategy_coinpair) " << strategy_coinpair << " (exchange_coinpair) " << exchange_coinpair);
				    //keyIsStrategyCoinpairWhiteList.insert(std::pair<std::string, std::string>(strategy_coinpair, exchange_coinpair));

                    //m_vstrSubscribeJsonString.push_back(makeSubscribeL2Update(exchange_coinpair));
                    //m_vstrSubscribeJsonString.push_back(makeSubscribeMatch(exchange_coinpair));
			}
		}
	}
}

std::string MDEngineKumex::getWhiteListCoinpairFrom(std::string md_coinpair)
{
    std::string& ticker = md_coinpair;
    //std::transform(ticker.begin(), ticker.end(), ticker.begin(), ::toupper);

    KF_LOG_INFO(logger, "[getWhiteListCoinpairFrom] find md_coinpair (md_coinpair) " << md_coinpair << " (toupper(ticker)) " << ticker);
    std::map<std::string, std::string>::iterator map_itr;
    map_itr = keyIsStrategyCoinpairWhiteList.begin();
    while(map_itr != keyIsStrategyCoinpairWhiteList.end()) {
		if(ticker == map_itr->second)
		{
            KF_LOG_INFO(logger, "[getWhiteListCoinpairFrom] found md_coinpair (strategy_coinpair) " << map_itr->first << " (exchange_coinpair) " << map_itr->second);
            return map_itr->first;
		}
        map_itr++;
    }
    KF_LOG_INFO(logger, "[getWhiteListCoinpairFrom] not found md_coinpair (md_coinpair) " << md_coinpair);
    return "";
}

void MDEngineKumex::debug_print(std::map<std::string, std::string> &keyIsStrategyCoinpairWhiteList)
{
	std::map<std::string, std::string>::iterator map_itr;
	map_itr = keyIsStrategyCoinpairWhiteList.begin();
	while(map_itr != keyIsStrategyCoinpairWhiteList.end()) {
		KF_LOG_INFO(logger, "[debug_print] keyIsExchangeSideWhiteList (strategy_coinpair) " << map_itr->first << " (md_coinpair) "<< map_itr->second);
		map_itr++;
	}
}

void MDEngineKumex::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineKumex::connect:");
    connected = true;
}

bool MDEngineKumex::getToken(Document& d) 
{
    int nTryCount = 0;
    cpr::Response response;
    do{
        //std::string url = "https://api.kumex.com/api/v1/bullet-public";
        std::string url = baseUrl + "/api/v1/bullet-public";
       response = Post(Url{url.c_str()}, Parameters{}); 
       
    }while(++nTryCount < rest_try_count && response.status_code != 200);

    if(response.status_code != 200)
    {
        KF_LOG_ERROR(logger, "MDEngineKumex::login::getToken Error");
        return false;
    }

    KF_LOG_INFO(logger, "MDEngineKumex::getToken: " << response.text.c_str());

    d.Parse(response.text.c_str());
    return true;
}


bool MDEngineKumex::getServers(Document& d)
{
    m_vstServerInfos.clear();
    m_strToken = "";
     if(d.HasMember("data"))
     {
         auto& data = d["data"];
         if(data.HasMember("token"))
         {
             m_strToken = data["token"].GetString();
             if(data.HasMember("instanceServers"))
             {
                 int nSize = data["instanceServers"].Size();
                for(int nPos = 0;nPos<nSize;++nPos)
                {
                    ServerInfo stServerInfo;
                    auto& server = data["instanceServers"].GetArray()[nPos];
                    if(server.HasMember("pingInterval"))
                    {
                        stServerInfo.nPingInterval = server["pingInterval"].GetInt();
                    }
                    if(server.HasMember("pingTimeOut"))
                    {
                        stServerInfo.nPingTimeOut = server["pingTimeOut"].GetInt();
                    }
                    if(server.HasMember("endpoint"))
                    {
                        stServerInfo.strEndpoint = server["endpoint"].GetString();
                    }
                    if(server.HasMember("protocol"))
                    {
                        stServerInfo.strProtocol = server["protocol"].GetString();
                    }
                    if(server.HasMember("encrypt"))
                    {
                        stServerInfo.bEncrypt = server["encrypt"].GetBool();
                    }
                    m_vstServerInfos.push_back(stServerInfo);
                }
             }
         }
     }
    if(m_strToken == "" || m_vstServerInfos.empty())
    {
        KF_LOG_ERROR(logger, "MDEngineKumex::login::getServers Error");
        return false;
    }
    return true;
}

std::string MDEngineKumex::getId()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  std::to_string(timestamp);
}

int64_t MDEngineKumex::getMSTime()
{
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return  timestamp;
}

void MDEngineKumex::login(long timeout_nsec)
{
	KF_LOG_INFO(logger, "MDEngineKumex::login:");

    Document d;
    if(!getToken(d))
    {
        return;
    }
    if(!getServers(d))
   {
       return;
   }
    m_nSubscribePos = 0;
	global_md = this;
	int inputPort = 8443;
	int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;

	struct lws_context_creation_info ctxCreationInfo;
	struct lws_client_connect_info clientConnectInfo;
	struct lws *wsi = NULL;
	struct lws_protocols protocol;

	memset(&ctxCreationInfo, 0, sizeof(ctxCreationInfo));
	memset(&clientConnectInfo, 0, sizeof(clientConnectInfo));

	ctxCreationInfo.port = CONTEXT_PORT_NO_LISTEN;
	ctxCreationInfo.iface = NULL;
	ctxCreationInfo.protocols = protocols;
	ctxCreationInfo.ssl_cert_filepath = NULL;
	ctxCreationInfo.ssl_private_key_filepath = NULL;
	ctxCreationInfo.extensions = NULL;
	ctxCreationInfo.gid = -1;
	ctxCreationInfo.uid = -1;
	ctxCreationInfo.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
	ctxCreationInfo.fd_limit_per_thread = 1024;
	ctxCreationInfo.max_http_header_pool = 1024;
	ctxCreationInfo.ws_ping_pong_interval=1;
	ctxCreationInfo.ka_time = 10;
	ctxCreationInfo.ka_probes = 10;
	ctxCreationInfo.ka_interval = 10;

	protocol.name  = protocols[PROTOCOL_TEST].name;
	protocol.callback = &ws_service_cb;
	protocol.per_session_data_size = sizeof(struct session_data);
	protocol.rx_buffer_size = 0;
	protocol.id = 0;
	protocol.user = NULL;

	context = lws_create_context(&ctxCreationInfo);
	KF_LOG_INFO(logger, "MDEngineKumex::login: context created.");


	if (context == NULL) {
		KF_LOG_ERROR(logger, "MDEngineKumex::login: context is NULL. return");
		return;
	}

	// Set up the client creation info
    auto& stServerInfo = m_vstServerInfos.front();
	std::string strAddress = stServerInfo.strEndpoint;
    size_t nAddressEndPos = strAddress.find_last_of('/');
    std::string strPath = strAddress.substr(nAddressEndPos);
    strPath += "?token=";
    strPath += m_strToken;
    strPath += "&[connectId=" +  getId() +"]";
    strAddress = strAddress.substr(0,nAddressEndPos);
    strAddress = strAddress.substr(strAddress.find_last_of('/') + 1);
    clientConnectInfo.address = strAddress.c_str();
    clientConnectInfo.path = strPath.c_str(); // Set the info's path to the fixed up url path
	clientConnectInfo.context = context;
	clientConnectInfo.port = 443;
	clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
	clientConnectInfo.host =strAddress.c_str();
	clientConnectInfo.origin = strAddress.c_str();
	clientConnectInfo.ietf_version_or_minus_one = -1;
	clientConnectInfo.protocol = protocols[PROTOCOL_TEST].name;
	clientConnectInfo.pwsi = &wsi;

    KF_LOG_INFO(logger, "MDEngineKumex::login: address = " << clientConnectInfo.address << ",path = " << clientConnectInfo.path);

	wsi = lws_client_connect_via_info(&clientConnectInfo);
	if (wsi == NULL) {
		KF_LOG_ERROR(logger, "MDEngineKumex::login: wsi create error.");
		return;
	}
	KF_LOG_INFO(logger, "MDEngineKumex::login: wsi create success.");
	logged_in = true;
}

void MDEngineKumex::logout()
{
   KF_LOG_INFO(logger, "MDEngineKumex::logout:");
}

void MDEngineKumex::release_api()
{
   KF_LOG_INFO(logger, "MDEngineKumex::release_api:");
}

void MDEngineKumex::set_reader_thread()
{
	IMDEngine::set_reader_thread();

	ws_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKumex::loop, this)));
    //rest_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKumex::rest_loop, this)));
    //check_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineKumex::check_loop, this)));
}

void MDEngineKumex::subscribeMarketData(const vector<string>& instruments, const vector<string>& markets)
{
   KF_LOG_INFO(logger, "MDEngineKumex::subscribeMarketData:");
}

std::string MDEngineKumex::makeSubscribeL2Update(std::string& strSymbol)
{
    StringBuffer sbL2Update;
	Writer<StringBuffer> writer(sbL2Update);
	writer.StartObject();
	writer.Key("id");
	writer.String(getId().c_str());
	writer.Key("type");
	writer.String("subscribe");
	writer.Key("topic");
    std::string strTopic = "/contractMarket/level2:";
    strTopic += strSymbol;
	writer.String(strTopic.c_str());
	writer.Key("response");
	writer.String("true");
	writer.EndObject();
    std::string strL2Update = sbL2Update.GetString();

    return strL2Update;
}

std::string MDEngineKumex::makeSubscribeMatch(std::string& strSymbol)
{
     StringBuffer sbMacth;
	Writer<StringBuffer> writer1(sbMacth);
	writer1.StartObject();
	writer1.Key("id");
	writer1.String(getId().c_str());
	writer1.Key("type");
	writer1.String("subscribe");
	writer1.Key("topic");
    std::string strTopic1 = "/contractMarket/execution:";
    strTopic1 += strSymbol;
	writer1.String(strTopic1.c_str());
    writer1.Key("privateChannel");
	writer1.String("false");
	writer1.Key("response");
	writer1.String("true");
	writer1.EndObject();
    std::string strLMatch = sbMacth.GetString();

    return strLMatch;
}

std::string MDEngineKumex::makeSubscribeFunding(std::string& strSymbol)
{
     StringBuffer sbMacth;
    Writer<StringBuffer> writer1(sbMacth);
    writer1.StartObject();
    writer1.Key("id");
    writer1.String(getId().c_str());
    writer1.Key("type");
    writer1.String("subscribe");
    writer1.Key("topic");
    std::string strTopic1 = "/contract/instrument:";
    strTopic1 += strSymbol;
    writer1.String(strTopic1.c_str());
    /*writer1.Key("privateChannel");
    writer1.String("false");*/
    writer1.Key("response");
    writer1.String("true");
    writer1.EndObject();
    std::string strLMatch = sbMacth.GetString();

    return strLMatch;
}

int MDEngineKumex::lws_write_subscribe(struct lws* conn)
{
	//KF_LOG_INFO(logger, "MDEngineKumex::lws_write_subscribe: (subscribe_index)" << subscribe_index);

    if(keyIsStrategyCoinpairWhiteList.size() == 0) return 0;
    int ret = 0;
    if(m_nSubscribePos < m_vstrSubscribeJsonString.size())
    {
        std::string& strSubscribe = m_vstrSubscribeJsonString[m_nSubscribePos];
        unsigned char msg[512];
        memset(&msg[LWS_PRE], 0, 512-LWS_PRE);
        int length = strSubscribe.length();
        KF_LOG_INFO(logger, "MDEngineKumex::lws_write_subscribe: " << strSubscribe.c_str() << " ,len = " << length);
        strncpy((char *)msg+LWS_PRE, strSubscribe.c_str(), length);
        int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
        m_nSubscribePos++;
        lws_callback_on_writable(conn);
    }
    else
    {
        if(shouldPing)
        {
            isPong = false;
            Ping(conn);
        }
    }
    
    return ret;
}

std::string MDEngineKumex::dealDataSprit(const char* src)
{
     std::string strData = src;
     auto nPos = strData.find("\\");
     while(nPos != std::string::npos)
     {
        strData.replace(nPos,1,"");
        nPos = strData.find("\\");
     }

     return strData;
}

 void MDEngineKumex::onPong(struct lws* conn)
 {
     Ping(conn);
 }

 void MDEngineKumex::Ping(struct lws* conn)
 {
     shouldPing = false;
    StringBuffer sbPing;
	Writer<StringBuffer> writer(sbPing);
	writer.StartObject();
	writer.Key("id");
	writer.String(getId().c_str());
	writer.Key("type");
	writer.String("ping");
	writer.EndObject();
    std::string strPing = sbPing.GetString();
    unsigned char msg[512];
    memset(&msg[LWS_PRE], 0, 512-LWS_PRE);
     int length = strPing.length();
    KF_LOG_INFO(logger, "MDEngineKumex::lws_write_ping: " << strPing.c_str() << " ,len = " << length);
    strncpy((char *)msg+LWS_PRE, strPing.c_str(), length);
    int ret = lws_write(conn, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
 }

void MDEngineKumex::on_lws_data(struct lws* conn, const char* data, size_t len)
{
    //std::string strData = dealDataSprit(data);
	KF_LOG_INFO(logger, "MDEngineKumex::on_lws_data: " << data);
    Document json;
	json.Parse(data);

	if(!json.HasParseError() && json.IsObject() && json.HasMember("type") && json["type"].IsString())
	{
        if(strcmp(json["type"].GetString(), "welcome") == 0)
        {
            KF_LOG_INFO(logger, "MDEngineKumex::on_lws_data: welcome");
            lws_callback_on_writable(conn);
        }
        if(strcmp(json["type"].GetString(), "pong") == 0)
		{
			KF_LOG_INFO(logger, "MDEngineKumex::on_lws_data: pong");
           isPong = true;
           m_conn = conn;
		}
		if(strcmp(json["type"].GetString(), "message") == 0)
		{
            if(strcmp(json["subject"].GetString(), "level2") == 0)
		    {
			    KF_LOG_INFO(logger, "MDEngineKumex::on_lws_data: is onDepth");
                onDepth(json);
            }
            if(strcmp(json["subject"].GetString(), "match") == 0)
            {
                KF_LOG_INFO(logger, "MDEngineKumex::on_lws_data: is onFills");
                onFills(json);
            }      
            if(strcmp(json["subject"].GetString(), "funding.rate") == 0)
            {
                KF_LOG_INFO(logger, "MDEngineKumex::on_lws_data: is onFunding");
                onFunding(json);
            } 
            if(strcmp(json["subject"].GetString(), "mark.index.price") == 0)
            {
                KF_LOG_INFO(logger, "MDEngineKumex::on_lws_data: is onMarkprice");
                onMarkprice(json);
            }                           
		}	
	} else 
    {
		KF_LOG_ERROR(logger, "MDEngineKumex::on_lws_data . parse json error: " << data);
	}
}

void MDEngineKumex::on_lws_connection_error(struct lws* conn)
{
	KF_LOG_ERROR(logger, "MDEngineKumex::on_lws_connection_error.");
	//market logged_in false;
    logged_in = false;
    KF_LOG_ERROR(logger, "MDEngineKumex::on_lws_connection_error. login again.");
    //clear the price book, the new websocket will give 200 depth on the first connect, it will make a new price book
	//clearPriceBook();
    priceBook20Assembler.clearPriceBook();
    isPong = false;
    shouldPing = true;
	//no use it
    long timeout_nsec = 0;
    //reset sub
    subscribe_index = 0;

    login(timeout_nsec);
}

void MDEngineKumex::clearPriceBook()
{
     std::lock_guard<std::mutex> lck(*m_mutexPriceBookData);
    m_mapPriceBookData.clear();
    mapLastData.clear();
}

void MDEngineKumex::onFunding(Document& json)
{
    KF_LOG_INFO(logger, "processing funding data");

    if(!json.HasMember("data"))
    {
        KF_LOG_INFO(logger, "received funding does not have valid data");
        return;
    }

    auto& node = json["data"];
    
    std::string total = json["topic"].GetString();
    std::string symbol = total.erase(0,21);
    std::string ticker = getWhiteListCoinpairFrom(symbol);
    if(ticker.empty())
    {
        KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_funding: not in WhiteList , ignore it:" << symbol);
        return;
    }
    KF_LOG_INFO(logger, "received funding symbol is " << symbol << " and ticker is " << ticker);
        
    LFFundingField fundingdata;
    memset(&fundingdata, 0, sizeof(fundingdata));
    strcpy(fundingdata.InstrumentID, ticker.c_str());
    strcpy(fundingdata.ExchangeID, "kumex");

    fundingdata.Rate = node["fundingRate"].GetDouble();
    fundingdata.TimeStamp = node["timestamp"].GetInt64();
    fundingdata.Interval = node["granularity"].GetInt64();
    KF_LOG_INFO(logger,"Rate:"<<fundingdata.Rate);
    
    on_funding_update(&fundingdata);

}

void MDEngineKumex::onMarkprice(Document& json)
{
    KF_LOG_INFO(logger, "onMarkprice");

    if(!json.HasMember("data"))
    {
        KF_LOG_INFO(logger, "received funding does not have valid data");
        return;
    }

    auto& node = json["data"];
    
    std::string total = json["topic"].GetString();
    std::string symbol = total.erase(0,21);
    std::string ticker = getWhiteListCoinpairFrom(symbol);
    if(ticker.empty())
    {
        KF_LOG_INFO(logger, "MDEngineBinanceF::on_lws_funding: not in WhiteList , ignore it:" << symbol);
        return;
    }
    KF_LOG_INFO(logger, "received funding symbol is " << symbol << " and ticker is " << ticker);

    LFMarkPrice markprice;
    memset(&markprice, 0, sizeof(markprice));
    strcpy(markprice.InstrumentID, ticker.c_str());

    markprice.MarkPrice = std::round(node["markPrice"].GetDouble() * scale_offset);

    on_markprice(&markprice);

    LFPriceIndex priceindex;
    memset(&priceindex, 0, sizeof(priceindex));
    strcpy(priceindex.InstrumentID, ticker.c_str());
     
    priceindex.Price = std::round(node["indexPrice"].GetDouble() * scale_offset);

    int64_t timestamp = node["timestamp"].GetInt64();
    std::string timestampstr = std::to_string(timestamp);
    strcpy(priceindex.TimeStamp, timestampstr.c_str());
    
    on_priceindex(&priceindex);
}

void MDEngineKumex::onFills(Document& json)
{
    if(!json.HasMember("data"))
    {
        KF_LOG_ERROR(logger, "MDEngineKumex::[onFills] invalid market trade message");
        return;
    }
    std::string ticker;
    auto& jsonData = json["data"];
   if(jsonData.HasMember("symbol"))
    {
        ticker = jsonData["symbol"].GetString();
    }
    if(ticker.length() == 0) {
		KF_LOG_INFO(logger, "MDEngineKumex::onFills: invaild data");
		return;
    }
    
    std::string strInstrumentID = getWhiteListCoinpairFrom(ticker);
    if(strInstrumentID == "")
    {
        strInstrumentID = ticker;
    }

    LFL2TradeField trade;
    memset(&trade, 0, sizeof(trade));
    strcpy(trade.InstrumentID, strInstrumentID.c_str());
    strcpy(trade.ExchangeID, "kumex");
    int64_t ts = jsonData["ts"].GetInt64();
    KF_LOG_INFO(logger,"ts true");
    std::string strTakerOrderId = jsonData["takerOrderId"].GetString();
    std::string strMakerOrderId = jsonData["makerOrderId"].GetString();
    std::string strTradeId = jsonData["tradeId"].GetString();
    int64_t sequence = jsonData["sequence"].GetInt64();
    std::string strSequence = std::to_string(sequence);
    trade.TimeStamp = ts;
    std::string strTime = timestamp_to_formatISO8601(trade.TimeStamp/1000000);
    strncpy(trade.TradeTime, strTime.c_str(),sizeof(trade.TradeTime));
    strncpy(trade.MakerOrderID, strMakerOrderId.c_str(),sizeof(trade.MakerOrderID));
    strncpy(trade.TakerOrderID, strTakerOrderId.c_str(),sizeof(trade.TakerOrderID));
    strncpy(trade.TradeID, strTradeId.c_str(),sizeof(trade.TradeID));
    strncpy(trade.Sequence, strSequence.c_str(),sizeof(trade.Sequence));
    /*
    trade.Price = std::round(std::stod(jsonData["price"].GetString()) * scale_offset);
    trade.Volume = std::round(std::stod(jsonData["size"].GetString()) * scale_offset);*/
    if(jsonData["price"].IsInt()){
        trade.Price = (int64_t)(jsonData["price"].GetInt()) * scale_offset;
    }else{
        trade.Price = (int64_t)(jsonData["price"].GetDouble()) * scale_offset;
    }
    KF_LOG_INFO(logger,"trade.Price ="<<trade.Price);
    trade.Volume = (uint64_t)(jsonData["size"].GetInt()) * scale_offset;
    KF_LOG_INFO(logger,"trade.Volume ="<<trade.Volume);
    static const string strBuy = "buy" ;
    trade.OrderBSFlag[0] = (strBuy == jsonData["side"].GetString()) ? 'B' : 'S';

    KF_LOG_INFO(logger, "MDEngineKumex::[onFills] (ticker)" << ticker <<
                                                                " (Price)" << trade.Price <<
                                                                " (Volume)" << trade.Volume << 
                                                                "(OrderBSFlag)" << trade.OrderBSFlag);
    on_trade(&trade);
}

bool MDEngineKumex::shouldUpdateData(const LFPriceBook20Field& md)
{
    bool has_update = false;
    auto it = mapLastData.find (md.InstrumentID);
    if(it == mapLastData.end())
    {
        mapLastData[md.InstrumentID] = md;
         has_update = true;
    }
     else
     {
        LFPriceBook20Field& lastMD = it->second;
        if(md.BidLevelCount != lastMD.BidLevelCount)
        {
            has_update = true;
        }
        else
        {
            for(int i = 0;i < md.BidLevelCount; ++i)
            {
                if(md.BidLevels[i].price != lastMD.BidLevels[i].price || md.BidLevels[i].volume != lastMD.BidLevels[i].volume)
                {
                    has_update = true;
                    break;
                }
            }
        }
        if(!has_update && md.AskLevelCount != lastMD.AskLevelCount)
        {
            has_update = true;
        }
        else if(!has_update)
        {
            for(int i = 0;i < md.AskLevelCount ;++i)
            {
                if(md.AskLevels[i].price != lastMD.AskLevels[i].price || md.AskLevels[i].volume != lastMD.AskLevels[i].volume)
                {
                    has_update = true;
                    break;
                }
            }
        }
        if(has_update)
        {
             mapLastData[md.InstrumentID] = md;
        }
    }	

    return has_update;
}

bool MDEngineKumex::getInitPriceBook(const std::string& strSymbol,std::map<std::string,PriceBookData>::iterator& itPriceBookData)
{
    int nTryCount = 0;
    cpr::Response response;
    //std::string url = "https://api.kumex.com/api/v1/level2/snapshot?symbol=";
    std::string url = baseUrl + "/api/v1/level2/snapshot?symbol=";
    url += strSymbol;
    //KF_LOG_INFO(logger,"getInitPriceBook url="<<url);

    do{  
       response = Get(Url{url.c_str()}, Parameters{}); 
       
    }while(++nTryCount < rest_try_count && response.status_code != 200);

    if(response.status_code != 200)
    {
        KF_LOG_ERROR(logger, "MDEngineKumex::login::getInitPriceBook Error, response = " <<response.text.c_str());
        return false;
    }
    KF_LOG_INFO(logger, "MDEngineKumex::getInitPriceBook: " << response.text.c_str());

    std::string ticker = getWhiteListCoinpairFrom(strSymbol);
    if(ticker == "")
    {
        ticker = strSymbol;
    }
    priceBook20Assembler.clearPriceBook(ticker);

    Document d;
    d.Parse(response.text.c_str());
    itPriceBookData = m_mapPriceBookData.insert(std::make_pair(ticker,PriceBookData())).first;
    if(!d.HasMember("data"))
    {
        return  true;
    }
    auto& jsonData = d["data"];
    if(jsonData.HasMember("sequence"))
    {
        //itPriceBookData->second.nSequence = std::round(stod(jsonData["sequence"].GetString()));
        itPriceBookData->second.nSequence = jsonData["sequence"].GetInt64();
    }
    if(jsonData.HasMember("bids"))
    {
        auto& bids =jsonData["bids"];
         if(bids .IsArray()) 
         {
             //fix for book_depth_count
             int len = std::min((int)bids.Size(), book_depth_count);
                //int len = bids.Size();
                for(int i = 0 ; i < len; i++)
                {
                    //int64_t price = std::round(stod(bids.GetArray()[i][0].GetString()) * scale_offset);
                    //uint64_t volume = std::round(stod(bids.GetArray()[i][1].GetString()) * scale_offset);
                    int64_t price = (int64_t)(bids.GetArray()[i][0].GetDouble() * scale_offset);
                    //uint64_t volume = (uint64_t)(bids.GetArray()[i][1].GetInt() * scale_offset);
                    int volume1 = bids.GetArray()[i][1].GetInt();
                    uint64_t volume = (uint64_t)volume1 * scale_offset;
                    //KF_LOG_INFO(logger,"volume1="<<volume1<<"volume="<<volume);
                    //itPriceBookData->second.mapBidPrice[price] = volume;
                    priceBook20Assembler.UpdateBidPrice(ticker, price, volume);
                }
         }
    }
    if(jsonData.HasMember("asks"))
    {
        auto& asks =jsonData["asks"];
         if(asks .IsArray()) 
         {
             //fix for book_depth_count
             int len = std::min((int)asks.Size(), book_depth_count);
                //int len = asks.Size();
                for(int i = 0 ; i < len; i++)
                {
                    //int64_t price = std::round(stod(asks.GetArray()[i][0].GetString()) * scale_offset);
                    //uint64_t volume = std::round(stod(asks.GetArray()[i][1].GetString()) * scale_offset);
                    int64_t price = (int64_t)(asks.GetArray()[i][0].GetDouble() * scale_offset);
                    //uint64_t volume = (uint64_t)(asks.GetArray()[i][1].GetInt() * scale_offset);
                    int volume1 = asks.GetArray()[i][1].GetInt();
                    uint64_t volume = (uint64_t)volume1 * scale_offset;
                    //KF_LOG_INFO(logger,"volume2="<<volume1<<"volume="<<volume);                    
                    //itPriceBookData->second.mapAskPrice[price] = volume;
                    priceBook20Assembler.UpdateAskPrice(ticker, price, volume);
                }
         }
    }
    KF_LOG_INFO(logger,"ticker="<<ticker);
     //printPriceBook(itPriceBookData->second);

    return true;
}

void MDEngineKumex::printPriceBook(const PriceBookData& stPriceBookData)
{
    std::stringstream ss;
    ss << "Bids[";
    for(auto it = stPriceBookData.mapBidPrice.rbegin(); it != stPriceBookData.mapBidPrice.rend();++it)
    {
        ss <<  "[" << it->first << "," << it->second << "],";
    }
    ss << "],Ask[";
     for(auto& pair : stPriceBookData.mapAskPrice)
    {
        ss <<  "[" << pair.first << "," << pair.second << "],";
    }
    ss << "].";

    KF_LOG_INFO(logger, "MDEngineKumex::printPriceBook: " << ss.str());
}

void MDEngineKumex::clearVaildData(PriceBookData& stPriceBookData)
{
     KF_LOG_INFO(logger, "MDEngineKumex::clearVaildData: ");
    for(auto it = stPriceBookData.mapAskPrice.begin();it !=stPriceBookData.mapAskPrice.end();)
    {
        if(it->first == 0 || it->second == 0)
        {
            it = stPriceBookData.mapAskPrice.erase(it);
        }
        else
        {
            ++it;
        }        
    }

    for(auto it = stPriceBookData.mapBidPrice.begin();it !=stPriceBookData.mapBidPrice.end();)
    {
        if(it->first == 0 || it->second == 0)
        {
            it = stPriceBookData.mapBidPrice.erase(it);
        }
        else
        {
            ++it;
        }   
    }
    //printPriceBook(stPriceBookData);
}

void MDEngineKumex::onDepth(Document& dJson)
{
    bool update = false;

    std::string ticker;
    if(!dJson.HasMember("data"))
    {
        return;
    }
    
    auto& jsonData = dJson["data"];
    
    ticker = dJson["topic"].GetString();
    int flag = ticker.find(":");
    ticker = ticker.substr(flag+1,ticker.length());
    
    KF_LOG_INFO(logger, "MDEngineKumex::onDepth start");

    if(ticker.length() == 0) {
		KF_LOG_INFO(logger, "MDEngineKumex::onDepth: invaild data");
		return;
    }
    
    KF_LOG_INFO(logger, "MDEngineKumex::onDepth:" << "(ticker) " << ticker);
    std::string strInstrumentID = getWhiteListCoinpairFrom(ticker);
    if(strInstrumentID == "")
    {
        strInstrumentID = ticker;
    }
    std::lock_guard<std::mutex> lck(*m_mutexPriceBookData);
    auto itPriceBook = m_mapPriceBookData.find(strInstrumentID);
    if(itPriceBook == m_mapPriceBookData.end())
    {
        if(!getInitPriceBook(ticker,itPriceBook))
        {
            KF_LOG_INFO(logger,"1MDEngineKumex::onDepth:return");
            return;
        }
    }

    if(jsonData.HasMember("sequence"))
    {
        auto sequence = jsonData["sequence"].GetInt64();
        KF_LOG_INFO(logger, "lichengyi-kumex::start to compare");
        while(1)
        {
            if(itPriceBook->second.nSequence + 1 < sequence)
            {
                string errorMsg = "Orderbook update sequence missed, request for a new snapshot";
                write_errormsg(5,errorMsg);
                KF_LOG_ERROR(logger, "lichengyi-kumex::Orderbook update missing "<< itPriceBook->second.nSequence<<"-" << sequence);

                if(!getInitPriceBook(ticker,itPriceBook))
                {
                    KF_LOG_INFO(logger,"2MDEngineKumex::onDepth:return");
                    return;
                }
            }
            else if(itPriceBook->second.nSequence >= sequence)
            {
                KF_LOG_INFO(logger, "lichengyi-kumex::onDepth:  old data,last sequence:" << itPriceBook->second.nSequence << ">= now sequence:" << sequence);
                return;
            }
            else
            {
                KF_LOG_INFO(logger, "lichengyi-kumex:No error" << itPriceBook->second.nSequence << "-" << sequence);
                break;
            }            
        }
    }

    KF_LOG_INFO(logger,"update");
    if(jsonData.HasMember("change"))
    {
        std::string change = jsonData["change"].GetString();
        int f1=change.find(",");
        std::string pricestr=change.substr(0,f1);
        //获取price的string
        change=change.substr(f1+1,change.length());
        int f2=change.find(",");
        std::string direction=change.substr(0,f2);
        //获取的sell或者buy
        std::string volumestr=change.substr(f2+1,change.length());
        //获取的第三个参数，订单数量
        int iprice=stoi(pricestr);
        double dvolume=stod(volumestr);

        if(direction == "sell") 
        {
            KF_LOG_INFO(logger,"is sell");      
            int64_t price = (int64_t)iprice * scale_offset;
            uint64_t volume = (uint64_t)dvolume * scale_offset;
            //KF_LOG_INFO(logger,"price="<<price<<"volume="<<volume);
            //itPriceBook->second.mapAskPrice[price] = volume;
            if(volume == 0){
                priceBook20Assembler.EraseAskPrice(strInstrumentID, price);
            }else{
                priceBook20Assembler.UpdateAskPrice(strInstrumentID,price,volume);
            }
        }
        else{
            KF_LOG_INFO(logger,"is buy");
            int64_t price = (int64_t)iprice * scale_offset;
            uint64_t volume = (uint64_t)dvolume * scale_offset;
            //KF_LOG_INFO(logger,"price="<<price<<"volume="<<volume);
            //itPriceBook->second.mapBidPrice[price] = volume;
            if(volume == 0){
                priceBook20Assembler.EraseBidPrice(strInstrumentID, price);
            }else{
                priceBook20Assembler.UpdateBidPrice(strInstrumentID,price,volume);
            }
        }
    }
    
    //printPriceBook(itPriceBook->second);

    if(jsonData.HasMember("sequence"))
    {
        itPriceBook->second.nSequence = std::round(jsonData["sequence"].GetInt64());
        KF_LOG_INFO(logger, "MDEngineKumex::onDepth:  sequence = " << itPriceBook->second.nSequence);
        //此处是更新snapshot的sequence

    }
    //clearVaildData(itPriceBook->second);

    LFPriceBook20Field md;
    memset(&md, 0, sizeof(md));
    //std::string strInstrumentID = getWhiteListCoinpairFrom(ticker);
    if(strInstrumentID.empty())
    {
        strInstrumentID = ticker;
    }
    strcpy(md.InstrumentID, strInstrumentID.c_str());
    strcpy(md.ExchangeID, "kumex");

    /*std::vector<PriceAndVolume> vstAskPriceAndVolume;
    std::vector<PriceAndVolume> vstBidPriceAndVolume;
    sortMapByKey(itPriceBook->second.mapAskPrice,vstAskPriceAndVolume,sort_price_asc);
    sortMapByKey(itPriceBook->second.mapBidPrice,vstBidPriceAndVolume,sort_price_desc);
    
    size_t nAskLen = 0;
    for(size_t nPos=0;nPos < vstAskPriceAndVolume.size();++nPos)
    {
        if(nAskLen >= book_depth_count)
        {
            break;
        }
        md.AskLevels[nPos].price = vstAskPriceAndVolume[nPos].price;
        md.AskLevels[nPos].volume = vstAskPriceAndVolume[nPos].volume;
        ++nAskLen ;
    }
    md.AskLevelCount = nAskLen;  

    size_t nBidLen = 0;
    for(size_t nPos=0;nPos < vstBidPriceAndVolume.size();++nPos)
    {
        if(nBidLen >= book_depth_count)
        {
            break;
        }
        md.BidLevels[nPos].price = vstBidPriceAndVolume[nPos].price;
        md.BidLevels[nPos].volume = vstBidPriceAndVolume[nPos].volume;
        ++nBidLen ;
    }
    md.BidLevelCount = nBidLen;*/  

    //if(shouldUpdateData(md))
    if(priceBook20Assembler.Assembler(strInstrumentID, md))
    {
        KF_LOG_INFO(logger, "MDEngineKumex::onDepth: on_price_book_update," << strInstrumentID << ",kumex");
        md.UpdateMicroSecond = itPriceBook->second.nSequence;
        /*edited by zyy,starts here*/
        //timer = getTimestamp();
        std::unique_lock<std::mutex> lck(book_mutex);
        auto it = control_book_map.find(ticker);
        if(it != control_book_map.end())
        {
            it->second = getTimestamp();
        }
        lck.unlock();

        KF_LOG_INFO(logger, "MDEngineKumex::onBook: BidLevelCount="<<md.BidLevelCount<<",AskLevelCount="<<md.AskLevelCount<<",level_threshold="<<level_threshold);
        if(md.BidLevelCount < level_threshold || md.AskLevelCount < level_threshold)
        {
            string errorMsg = "orderbook level below threshold";
            write_errormsg(112,errorMsg);
            on_price_book_update(&md);
        }
        else if(md.BidLevels[0].price <=0 || md.AskLevels[0].price <=0 || md.BidLevels[0].price > md.AskLevels[0].price)
        {
            string errorMsg = "orderbook crossed";
            write_errormsg(113,errorMsg);
        }
        /*edited by zyy ends here*/
        else
        {
            /*KF_LOG_INFO(logger,"sequence1="<<itPriceBook->second.nSequence);
            for(int i = 0; i < 20; i++){
                KF_LOG_INFO(logger,"bidprice="<<md.BidLevels[i].price<<" bisvol="<<md.BidLevels[i].volume<<
                    " askprice="<<md.AskLevels[i].price<<" askvol="<<md.AskLevels[i].volume);
            }*/
            BookMsg bookmsg;
            bookmsg.book = md;
            bookmsg.time = getTimestamp();
            bookmsg.sequence = itPriceBook->second.nSequence;

            std::unique_lock<std::mutex> lck1(ws_book_mutex);
            auto itr = ws_book_map.find(strInstrumentID);
            if(itr == ws_book_map.end()){
                std::vector<BookMsg> bookmsg_vec;
                bookmsg_vec.push_back(bookmsg);                
                ws_book_map.insert(make_pair(strInstrumentID, bookmsg_vec));
            }else{
                itr->second.push_back(bookmsg);
            }
            lck1.unlock();
            /*std::unique_lock<std::mutex> lck2(update_mutex);
            auto it = has_bookupdate_map.find(strInstrumentID);
            if(it == has_bookupdate_map.end()){
                KF_LOG_INFO(logger,"insert"<<strInstrumentID);
                has_bookupdate_map.insert(make_pair(strInstrumentID, itPriceBook->second.nSequence));
            }
            lck2.unlock();*/
            std::unique_lock<std::mutex> lck4(book_mutex);
            book_map[strInstrumentID] = bookmsg;
            lck4.unlock();

            KF_LOG_INFO(logger,"ws sequence="<<itPriceBook->second.nSequence);            
            on_price_book_update(&md);
        }
    }
    else 
    { 
        KF_LOG_INFO(logger, "MDEngineKumex::onDepth: same data not update" );
        std::unique_lock<std::mutex> lck4(book_mutex);
        auto it = book_map.find(strInstrumentID);
        if(it != book_map.end()){
            BookMsg bookmsg;
            bookmsg.book = it->second.book;
            bookmsg.time = getTimestamp();
            bookmsg.sequence = itPriceBook->second.nSequence;
            std::unique_lock<std::mutex> lck1(ws_book_mutex);
            auto itr = ws_book_map.find(strInstrumentID);
            if(itr != ws_book_map.end()){
                itr->second.push_back(bookmsg);
            }
            lck1.unlock(); 
            KF_LOG_INFO(logger,"sequence3="<<itPriceBook->second.nSequence);
            /*for(int i = 0; i < 20; i++){
                KF_LOG_INFO(logger,"bidprice="<<bookmsg.book.BidLevels[i].price<<" bisvol="<<bookmsg.book.BidLevels[i].volume<<
                    " askprice="<<bookmsg.book.AskLevels[i].price<<" askvol="<<bookmsg.book.AskLevels[i].volume);
            } */
        }
        lck4.unlock();
    }
}

std::string MDEngineKumex::parseJsonToString(const char* in)
{
	Document d;
	d.Parse(reinterpret_cast<const char*>(in));

	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	d.Accept(writer);

	return buffer.GetString();
}

void MDEngineKumex::get_snapshot_via_rest()
{
    {
        for(const auto& item : keyIsStrategyCoinpairWhiteList)
        {
            //std::string url = "https://api.kumex.com/api/v1/level2/snapshot?symbol=";
            std::string url = baseUrl + "/api/v1/level2/snapshot?symbol=";
            url+=item.second;
            cpr::Response response = Get(Url{url.c_str()}, Parameters{}); 
            Document d;
            d.Parse(response.text.c_str());
            KF_LOG_INFO(logger, "get_snapshot_via_rest get("<< url << "):" << response.text);
            //"code":"200000"
            if(d.IsObject() && d.HasMember("code") && d["code"].GetString() == std::string("200000") && d.HasMember("data"))
            {
                auto& tick = d["data"];
                LFPriceBook20Field priceBook {0};
                strcpy(priceBook.ExchangeID, "kumex");
                strncpy(priceBook.InstrumentID, item.first.c_str(),std::min(sizeof(priceBook.InstrumentID)-1, item.first.size()));
                if(tick.HasMember("bids") && tick["bids"].IsArray())
                {
                    auto& bids = tick["bids"];
                    //fix for book_depth_count
                    int len = std::min((int)bids.Size(), book_depth_count);
                    //int len = std::min((int)bids.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.BidLevels[i].price = std::round(bids[i][0].GetDouble() * scale_offset);
                        int64_t volume = std::round(bids[i][1].GetInt64() * scale_offset);
                        priceBook.BidLevels[i].volume = volume;
                    }
                    priceBook.BidLevelCount = len;
                }
                if (tick.HasMember("asks") && tick["asks"].IsArray())
                {
                    auto& asks = tick["asks"];
                    //fix for book_depth_count
                    int len = std::min((int)asks.Size(), book_depth_count);
                    //int len = std::min((int)asks.Size(),20);
                    for(int i = 0; i < len; ++i)
                    {
                        priceBook.AskLevels[i].price = std::round(asks[i][0].GetDouble() * scale_offset);
                        int64_t volume = std::round(asks[i][1].GetInt64() * scale_offset);
                        priceBook.AskLevels[i].volume = volume;
                    }
                    priceBook.AskLevelCount = len;
                }
                if(tick.HasMember("sequence"))
                {
                    priceBook.UpdateMicroSecond = std::round(tick["sequence"].GetInt64());
                }

                BookMsg bookmsg;
                bookmsg.time = getTimestamp();
                bookmsg.book = priceBook;
                bookmsg.sequence = std::round(tick["sequence"].GetInt64());
                std::unique_lock<std::mutex> lck3(rest_book_mutex);
                rest_book_vec.push_back(bookmsg);    
                lck3.unlock();           

                on_price_book_update_from_rest(&priceBook);
            }
        }
    }
    

}

void MDEngineKumex::check_snapshot()
{
    std::vector<BookMsg>::iterator rest_it;
    std::unique_lock<std::mutex> lck3(rest_book_mutex);
    for(rest_it = rest_book_vec.begin();rest_it != rest_book_vec.end();){
        bool has_same_book = false;
        int64_t now = getTimestamp();
        //bool has_start = false;
        //KF_LOG_INFO(logger,"string(rest_it->book.InstrumentID)"<<string(rest_it->book.InstrumentID));
        std::unique_lock<std::mutex> lck1(ws_book_mutex);
        auto map_itr = ws_book_map.find(string(rest_it->book.InstrumentID));
        if(map_itr != ws_book_map.end()){
            int64_t sequence_time;
            bool has_start = false;
            std::vector<BookMsg>::iterator ws_it;
            for(ws_it = map_itr->second.begin(); ws_it != map_itr->second.end(); ws_it++){
                if(ws_it->sequence == rest_it->sequence){
                    has_start = true;
                    sequence_time = ws_it->time;
                    break;
                }
            }
            if(!has_start){
                KF_LOG_INFO(logger,"not start:"<<rest_it->book.InstrumentID<<" "<<rest_it->sequence);
                rest_it = rest_book_vec.erase(rest_it);
                continue;
            }
            
            for(ws_it = map_itr->second.begin(); ws_it != map_itr->second.end();){
                //KF_LOG_INFO(logger,"sequence_1="<<ws_it->sequence<<" sequence_2="<<rest_it->sequence);
                if(now - ws_it->time > 10000){
                    KF_LOG_INFO(logger,"erase old");
                    ws_it = map_itr->second.erase(ws_it);
                    continue;
                }
                if(ws_it->sequence >= rest_it->sequence && ws_it->time - sequence_time <= 1000){
                    //if(ws_it->time - sequence_time <= 1000){
                        bool same_book = true;
                        for(int i = 0; i < 20; i++ ){
                            if(ws_it->book.BidLevels[i].price != rest_it->book.BidLevels[i].price || ws_it->book.BidLevels[i].volume != rest_it->book.BidLevels[i].volume || 
                               ws_it->book.AskLevels[i].price != rest_it->book.AskLevels[i].price || ws_it->book.AskLevels[i].volume != rest_it->book.AskLevels[i].volume)
                            {
                                same_book = false;
                                //has_error = true;
                                /*KF_LOG_INFO(logger, "2ws snapshot is not same as rest snapshot.sequence = "<< rest_it->sequence);
                                KF_LOG_INFO(logger,"ws_it:"<<ws_it->book.BidLevels[i].price<<" "<<ws_it->book.BidLevels[i].volume<<
                                    " "<<ws_it->book.AskLevels[i].price<<" "<<ws_it->book.AskLevels[i].volume);
                                KF_LOG_INFO(logger,"rest_it:"<<rest_it->book.BidLevels[i].price<<" "<<rest_it->book.BidLevels[i].volume<<
                                    " "<<rest_it->book.AskLevels[i].price<<" "<<rest_it->book.AskLevels[i].volume);
                                string errorMsg = "ws snapshot is not same as rest snapshot";
                                write_errormsg(116,errorMsg); */                          
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
                            break;
                        }else{
                            ws_it++;
                        }                        
                    //}
                }else{
                    ws_it++;
                }
            }
        }
        lck1.unlock();
        if(!has_same_book){
            if(now - rest_it->time > snapshot_check_s * 1000)
            {
                KF_LOG_INFO(logger, "ws snapshot is not same as rest snapshot.sequence = "<< rest_it->sequence);
                rest_it = rest_book_vec.erase(rest_it);
                string errorMsg = "ws snapshot is not same as rest snapshot";
                write_errormsg(116,errorMsg);   
            }/*else if(has_error){
                rest_it = rest_book_vec.erase(rest_it);
            }*/
            else{
                rest_it++;
            }                
        }else{
            KF_LOG_INFO(logger, "check good");
            rest_it = rest_book_vec.erase(rest_it);
        }                
    }
    lck3.unlock();
}

int64_t last_rest_time = 0;
void MDEngineKumex::rest_loop()
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
void MDEngineKumex::check_loop()
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

void MDEngineKumex::loop()
{
        time_t nLastTime = time(0);

		while(isRunning)
		{
            /*edited by zyy,starts here*/
            int64_t now = getTimestamp();
            int errorId = 0;
            std::string errorMsg = "";
            /*KF_LOG_INFO(logger,"now = "<<now<<",timer = "<<timer<<", refresh_normal_check_book_s="<<refresh_normal_check_book_s);
            if ((now - timer) > refresh_normal_check_book_s * 1000)
            {
                KF_LOG_INFO(logger, "failed price book update");
                write_errormsg(114,"orderbook max refresh wait time exceeded");
                timer = now;
            }*/
            std::unique_lock<std::mutex> lck(book_mutex);
            std::map<std::string,int64_t>::iterator it;
            for(it = control_book_map.begin(); it != control_book_map.end(); it++){
                if((now - it->second) > refresh_normal_check_book_s * 1000){
	                errorId = 114;
	                errorMsg = it->first + " orderbook max refresh wait time exceeded";
	                KF_LOG_INFO(logger,"114"<<errorMsg); 
	                write_errormsg(errorId,errorMsg);
	                it->second = now;           		
                }
            } 
            lck.unlock();
            /*edited by zyy ends here*/
            time_t nNowTime = time(0);
            if(isPong && (nNowTime - nLastTime>= 30))
            {
                isPong = false;
                 nLastTime = nNowTime;
                 KF_LOG_INFO(logger, "MDEngineKumex::loop: last time = " <<  nLastTime << ",now time = " << nNowTime << ",isPong = " << isPong);
                shouldPing = true;
                lws_callback_on_writable(m_conn);  
            }
            //KF_LOG_INFO(logger, "MDEngineKumex::loop:lws_service");
			lws_service( context, rest_get_interval_ms );
		}
}

/*edited by zyy,starts here*/
inline int64_t MDEngineKumex::getTimestamp()
{   /*返回的是毫秒*/
    long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return timestamp;
}
/*edited by zyy ends here*/

BOOST_PYTHON_MODULE(libkumexmd)
{
    using namespace boost::python;
    class_<MDEngineKumex, boost::shared_ptr<MDEngineKumex> >("Engine")
    .def(init<>())
    .def("init", &MDEngineKumex::initialize)
    .def("start", &MDEngineKumex::start)
    .def("stop", &MDEngineKumex::stop)
    .def("logout", &MDEngineKumex::logout)
    .def("wait_for_stop", &MDEngineKumex::wait_for_stop);
}