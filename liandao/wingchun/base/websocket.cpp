#include "websocket.h"
#include <map>
#include <iostream>
using namespace kungfu::wingchun::common;
using namespace std;
std::mutex g_mutex;
map<lws_context *,CWebsocket*> g_mapLwsInstance;

static int ws_service_cb( struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len )
{
    std::string strMsg((const char*)in,len);
    cout << "reason:" << reason << ",msg:"<< strMsg << ",connection:"<< wsi << ",thread:"<< this_thread::get_id() << endl;
    std::unique_lock<std::mutex> lck(g_mutex);
    lws_context * _context = lws_get_context(wsi);
    auto it  = g_mapLwsInstance.find(_context);
    if(it == g_mapLwsInstance.end())
    {
        cout << "return no wsi" << endl;
        return 0;
    }
    CWebsocket* ptrWs = it->second;
    lck.unlock();
    switch( reason )
    {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            cout << "LWS_CALLBACK_CLIENT_ESTABLISHED." << endl;
            if(ptrWs->m_ptrCallback != nullptr)
            {
                ptrWs->m_ptrCallback->OnConnected(ptrWs);
            }
            break;
        }
        case LWS_CALLBACK_PROTOCOL_INIT:
        {
             cout << "LWS_CALLBACK_PROTOCOL_INIT." << endl;
            
            break;
        }
        case LWS_CALLBACK_CLIENT_RECEIVE:
        {
            if(ptrWs->m_ptrCallback != nullptr)
            {
                ptrWs->m_ptrCallback->OnReceivedMessage(ptrWs,std::string((const char*)in,len));
            }
            break;
        }
        case LWS_CALLBACK_CLIENT_WRITEABLE:
        {
            cout << "LWS_CALLBACK_CLIENT_WRITEABLE." <<endl;
            ptrWs->SendMessageInner();
            break;
        }
        case LWS_CALLBACK_CLOSED:
        //case LWS_CALLBACK_WSI_DESTROY:
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            cout << "lws_callback,reason=" << reason << endl;
            
             if(ptrWs->m_ptrCallback != nullptr)
            {
                ptrWs->m_ptrCallback->OnDisconnected(ptrWs);
            }
            break;
        }
        default:
              //if(global_md) global_md->writeErrorLog(ss.str());
            break;
    }
    //cout << "return normal" << endl;
    return 0;
}

static struct lws_protocols protocols[] =
{
        {
            "ws-protocol",
            ws_service_cb,
            0,
            65536,
        },
        { NULL, NULL, 0, 0 } /* terminator */
};
CWebsocket::CWebsocket(/* args */)
{
    m_bIsRunning = true;
    m_mutexQueue = new std::mutex;
}
CWebsocket::CWebsocket(const CWebsocket& src)
{
    m_bIsRunning = true;
    m_mutexQueue = new std::mutex;
    m_ptrCallback=src.m_ptrCallback;
    m_strUrl = src.m_strUrl;
    m_nPort = src.m_nPort;
}           
CWebsocket::~CWebsocket()
{
    Close();
    delete m_mutexQueue;
}
bool CWebsocket::Connect(const std::string& url,int port )
{
    Close();
    int logs = LLL_ERR | LLL_DEBUG | LLL_WARN;

    struct lws_context_creation_info ctxCreationInfo;
    struct lws_client_connect_info clientConnectInfo;
    const char *urlProtocol, *urlTempPath;
    char urlPath[300]={0};
    char inputURL[300]={0};
    strcpy(inputURL,url.c_str());
    //char* inputURL = (char*)url.c_str();
    memset(&ctxCreationInfo, 0, sizeof(ctxCreationInfo));
    memset(&clientConnectInfo, 0, sizeof(clientConnectInfo));

    if (lws_parse_uri(inputURL, &urlProtocol, &clientConnectInfo.address, &clientConnectInfo.port, &urlTempPath))
    {
        cout << "connect: Couldn't parse URL. Please check the URL and retry: " << inputURL << endl;;
        return false;
    }

    // Fix up the urlPath by adding a / at the beginning, copy the temp path, and add a \0     at the end
    urlPath[0] = '/';
    strncpy(urlPath + 1, urlTempPath, sizeof(urlPath) - 2);
    urlPath[sizeof(urlPath) - 1] = '\0';
    clientConnectInfo.path = urlPath; // Set the info's path to the fixed up url path

    cout << "urlProtocol=" << urlProtocol << "address=" << clientConnectInfo.address <<
                                                  "urlTempPath=" << urlTempPath <<"urlPath=" << urlPath << endl;

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

    bool isOK =  true;
    m_context = lws_create_context(&ctxCreationInfo);
    if (m_context == NULL) {
        cout << "context is NULL. return" << endl;
        return false;
    }
    else
    {
        std::unique_lock<std::mutex> lck(g_mutex);
        isOK =  g_mapLwsInstance.insert(std::make_pair(m_context,this)).second;
        lck.unlock();
        m_bIsRunning = true;
        //m_ptrThread = std::shared_ptr<std::thread>(new std::thread(&CWebsocket::loop,this));
        cout << "context created." << endl;
    }
    // Set up the client creation info
    clientConnectInfo.context = m_context;
    clientConnectInfo.port = port;
    clientConnectInfo.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_SELFSIGNED | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    clientConnectInfo.host = clientConnectInfo.address;
    clientConnectInfo.origin = clientConnectInfo.address;
    clientConnectInfo.ietf_version_or_minus_one = -1;
    clientConnectInfo.protocol = protocols[0].name;
    clientConnectInfo.pwsi = &m_connection;
    
    m_connection = lws_client_connect_via_info(&clientConnectInfo);
    if (m_connection == NULL) {
        cout << "wsi create error." << endl;
        return false;
    }
    cout << "connection:"<< m_connection << ",thread:"<< this_thread::get_id() << endl;
    
    //std::lock_guard<std::mutex> lck(g_mutex);
    //bool isOK =  g_mapLwsInstance.insert(std::make_pair(m_connection,this)).second;
    if(isOK)
    {
        m_strUrl = url;
        m_nPort = port;
    }

    StartThread();
    return isOK;
}

void CWebsocket::StartThread()
{
    cout << "StartThread." << endl;
    m_ptrThread = std::shared_ptr<std::thread>(new std::thread(&CWebsocket::loop,this));
}

void CWebsocket::SendMessage(const std::string& msg)
{
    std::unique_lock<std::mutex> lck(*m_mutexQueue);
    m_queueMsg.push(msg);
    lck.unlock();
    lws_callback_on_writable(m_connection);
}
void CWebsocket::RegisterCallBack(CWebsocketCallBack* callback)
{
    m_ptrCallback = callback;
}
void CWebsocket::Close()
{
    
    if(m_context == nullptr)
    {
        return;
    }
    m_bIsRunning = false;
    if(m_ptrThread->joinable())
    {
        m_ptrThread->join();
    }
    cout << "lws detroy" << endl;
    lws_context_destroy(m_context);
    std::lock_guard<std::mutex> lck(g_mutex);
    g_mapLwsInstance.erase(m_context);
    m_context = nullptr;
    for(auto it = g_mapLwsInstance.begin();it != g_mapLwsInstance.end();++it)
    {
        if(it->second == this)
        {
            g_mapLwsInstance.erase(it);
            break;
        }
    }
}
void CWebsocket::SendMessageInner()
{
    std::unique_lock<std::mutex> lck(*m_mutexQueue);
    if(m_queueMsg.size() == 0)
    {
        return;
    }
    std::string strMsg = m_queueMsg.front();
    m_queueMsg.pop();
    lck.unlock();
    unsigned char msg[1024];
    memset(&msg[LWS_PRE], 0, 1024-LWS_PRE);
    int length = strMsg.length();

    strncpy((char *)msg+LWS_PRE, strMsg.c_str(), length);
    int ret = lws_write(m_connection, &msg[LWS_PRE], length,LWS_WRITE_TEXT);
    if( m_queueMsg.size() > 0)
        lws_callback_on_writable(m_connection);
}
void CWebsocket::loop()
{
    while(m_bIsRunning)
    {
        if(m_ptrCallback != nullptr)
        {
            m_ptrCallback->DoLoopItem();
        }
        lws_service( m_context, 500 );
    }
}