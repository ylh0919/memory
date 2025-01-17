// auto generated by struct_info_parser.py, please DO NOT edit!!!

#ifndef LONGFIST_CONSTANTS_H
#define LONGFIST_CONSTANTS_H

#include <memory.h>
#include <cstdlib>
#include <stdio.h>
#include <string>
#include <map>
// Index for Sources...
enum exchange_source_index : int
{
    SOURCE_UNKNOWN = -1,
    SOURCE_CTP = 1,
    SOURCE_XTP = 15,
    SOURCE_BINANCE = 16,
    SOURCE_INDODAX = 17,
    SOURCE_OKEX = 18,
    SOURCE_COINMEX = 19,
    SOURCE_MOCK = 20,
    SOURCE_ASCENDEX = 21,
    SOURCE_BITFINEX = 22,
    SOURCE_BITMEX = 23,
    SOURCE_HITBTC = 24,
    SOURCE_OCEANEX = 25,
    SOURCE_HUOBI = 26,
    SOURCE_OCEANEXB = 27,
    SOURCE_PROBIT = 28,
    SOURCE_BITHUMB = 29,
    SOURCE_UPBIT = 30,
    SOURCE_DAYBIT = 31,
    SOURCE_KUCOIN = 32,
    SOURCE_BITFLYER = 33,
    SOURCE_KRAKEN = 34,
    SOURCE_IB = 35,
    SOURCE_BITTREX = 36,
    SOURCE_POLONIEX = 37,
    SOURCE_BITSTAMP = 38,
    SOURCE_DERIBIT = 39,
    SOURCE_EMX = 40,
    SOURCE_COINFLEX = 41,
    SOURCE_COINFLOOR = 42,
    SOURCE_MOCKKUCOIN = 43,
    SOURCE_MOCKBITMEX = 44,
    SOURCE_ERISX = 45,
    SOURCE_HBDM = 46,
    SOURCE_KUMEX = 47,
    SOURCE_BINANCEF = 48,
    SOURCE_BEQUANT = 49,
    SOURCE_COINCHECK = 50,
    SOURCE_KRAKENF = 51,
    SOURCE_OKEXF = 52,
    SOURCE_FTX = 53,
    SOURCE_MOCKHBDM = 153,
    SOURCE_MOCKCOINFLEX = 154,
    SOURCE_MOCKBINANCE = 155,
    SOURCE_MOCKDERIBIT = 156,
    SOURCE_GOOGLETRENDS = 57,
    SOURCE_QDP = 58,
    SOURCE_BINANCED = 59
};

inline const std::map<int,std::string> get_map_sources()
{
    std::map<int,std::string> mapSources = {
        {SOURCE_CTP,"ctp"},
        {SOURCE_XTP,"xtp"},
        {SOURCE_BINANCE,"binance"},
        {SOURCE_INDODAX,"indodax"},
        {SOURCE_OKEX,"okex"},
        {SOURCE_COINMEX,"coinmex"},
        {SOURCE_MOCK,"mock"},
        {SOURCE_ASCENDEX,"ascendex"},
        {SOURCE_BITFINEX,"bitfinex"},
        {SOURCE_BITMEX,"bitmex"},
        {SOURCE_HITBTC,"hitbtc"},
        {SOURCE_OCEANEX,"oceanex"},
        {SOURCE_HUOBI,"huobi"},
        {SOURCE_OCEANEXB,"oceanexb"},
        {SOURCE_PROBIT,"probit"},
        {SOURCE_BITHUMB,"bithumb"},
        {SOURCE_UPBIT,"upbit"},
        {SOURCE_DAYBIT,"daybit"},
        {SOURCE_KUCOIN,"kucoin"},
        {SOURCE_BITFLYER,"bitflyer"},
        {SOURCE_KRAKEN,"kraken"},
        {SOURCE_IB,"ib"},
        {SOURCE_BITTREX,"bittrex"},
        {SOURCE_POLONIEX,"poloniex"},
        {SOURCE_BITSTAMP,"bitstamp"},
        {SOURCE_DERIBIT,"deribit"},
        {SOURCE_EMX,"emx"},
        {SOURCE_COINFLEX,"coinflex"},
        {SOURCE_COINFLOOR,"coinfloor"},
        {SOURCE_MOCKKUCOIN,"mockkucoin"},
        {SOURCE_MOCKBITMEX,"mockbitmex"},
        {SOURCE_ERISX,"erisx"},
        {SOURCE_HBDM,"hbdm"},
        {SOURCE_KUMEX,"kumex"},
        {SOURCE_BINANCEF,"binancef"},
        {SOURCE_BEQUANT,"bequant"},
        {SOURCE_COINCHECK,"coincheck"},
        {SOURCE_KRAKENF,"krakenf"},
        {SOURCE_OKEXF,"okexf"},
        {SOURCE_FTX,"ftx"},
        {SOURCE_MOCKHBDM,"mockhbdm"},
        {SOURCE_MOCKBINANCE,"mockbinance"},
        {SOURCE_MOCKDERIBIT,"mockderibit"},
        {SOURCE_MOCKCOINFLEX,"mockcoinflex"},
        {SOURCE_GOOGLETRENDS,"googletrends"},
        {SOURCE_QDP,"qdp"},
        {SOURCE_BINANCED,"binanced"},
    };
    return mapSources;
}
inline const char* get_str_from_source_index(int source)
{
    std::map<int,std::string> mapSources = get_map_sources();
    auto it = mapSources.find(source);
    if(it != mapSources.end())
    {
        return it->second.c_str();
    }
    else
    {
        return "unknown";
    }
}

inline exchange_source_index get_source_index_from_str(const std::string& exch_str)
{
    std::map<int,std::string> mapSources = get_map_sources();
    for(auto& exch:mapSources)
    {
        if(exch_str == exch.second)
        {
            return (exchange_source_index)exch.first;
        }
    }
    return SOURCE_UNKNOWN;
}

// Exchange names
#define EXCHANGE_SSE "SSE" //上海证券交易所
#define EXCHANGE_SZE "SZE" //深圳证券交易所
#define EXCHANGE_CFFEX "CFFEX" //中国金融期货交易所
#define EXCHANGE_SHFE "SHFE" //上海期货交易所
#define EXCHANGE_DCE "DCE" //大连商品交易所
#define EXCHANGE_CZCE "CZCE" //郑州商品交易所
#define EXCHANGE_BINANCE "BINANCE"
#define EXCHANGE_INDODAX "INDODAX"
#define EXCHANGE_OKEX  "OKEX"
#define EXCHANGE_COINMEX "COINMEX"
#define EXCHANGE_MOCK "MOCK"
#define EXCHANGE_ASCENDEX "ASCENDEX"
#define EXCHANGE_BITFINEX "BITFINEX"
#define EXCHANGE_BITMEX "BITMEX"
#define EXCHANGE_HITBTC "HITBTC"
#define EXCHANGE_OCEANEX "OCEANEX"
#define EXCHANGE_HUOBI "HUOBI"
#define EXCHANGE_OCEANEXB "OCEANEXB"
#define EXCHANGE_PROBIT "PROBIT"
#define EXCHANGE_BITHUMB "BITHUMB"
#define EXCHANGE_UPBIT "UPBIT"
#define EXCHANGE_DAYBIT "DAYBIT"
#define EXCHANGE_KUCOIN "KUCOIN"
#define EXCHANGE_BITFLYER "BITFLYER"
#define EXCHANGE_KRAKEN "KRAKEN"
#define EXCHANGE_IB "IB"
#define EXCHANGE_BITTREX "BITTREX"
#define EXCHANGE_POLONIEX "POLONIEX"
#define EXCHANGE_BITSTAMP "BITSTAMP"
#define EXCHANGE_DERIBIT "DERIBIT"
#define EXCHANGE_EMX "EMX"
#define EXCHANGE_COINFLEX "COINFLEX"
#define EXCHANGE_COINFLOOR "COINFLOOR"
#define EXCHANGE_MOCKKUCOIN "MOCKKUCOIN"
#define EXCHANGE_MOCKBITMEX "MOCKBITMEX"
#define EXCHANGE_BINANCEF "BINANCEF"
#define EXCHANGE_HBDM "HBDM"
#define EXCHANGE_KUMEX "KUMEX"
#define EXCHANGE_ERISX "ERISX"
#define EXCHANGE_BEQUANT "BEQUANT"
#define EXCHANGE_KRAKENF "KRAKENF"
#define EXCHANGE_COINCHECK "COINCHECK"
#define EXCHANGE_OKEXF "OKEXF"
#define EXCHANGE_FTX "FTX"
#define EXCHANGE_MOCKHBDM "MOCKHBDM"
#define EXCHANGE_MOCKDERIBIT "MOCKDERIBIT"
#define EXCHANGE_MOCKCOINFLEX "MOCKCOINFLEX"
#define EXCHANGE_MOCKBINANCE "MOCKBINANCE"
#define EXCHANGE_GOOGLETRENDS "GOOGLETRENDS"
#define EXCHANGE_QDP "QDP"
#define EXCHANGE_BINANCED "BINANCED"

// Exchange ids
#define EXCHANGE_ID_SSE 1 //上海证券交易所
#define EXCHANGE_ID_SZE 2 //深圳证券交易所
#define EXCHANGE_ID_CFFEX 11 //中国金融期货交易所
#define EXCHANGE_ID_SHFE 12 //上海期货交易所
#define EXCHANGE_ID_DCE 13 //大连商品交易所
#define EXCHANGE_ID_CZCE 14 //郑州商品交易所
#define EXCHANGE_ID_BINANCE  16
#define EXCHANGE_ID_INDODAX  17
#define EXCHANGE_ID_OKEX  18
#define EXCHANGE_ID_COINMEX  19
#define EXCHANGE_ID_MOCK  20
#define EXCHANGE_ID_ASCENDEX  21
#define EXCHANGE_ID_BITFINEX  22
#define EXCHANGE_ID_BITMEX  23
#define EXCHANGE_ID_HITBTC  24
#define EXCHANGE_ID_OCEANEX  25
#define EXCHANGE_ID_HUOBI  26
#define EXCHANGE_ID_OCEANEXB  27
#define EXCHANGE_ID_PROBIT  28
#define EXCHANGE_ID_BITHUMB  29
#define EXCHANGE_ID_UPBIT   30
#define EXCHANGE_ID_DAYBIT  31
#define EXCHANGE_ID_KUCOIN  32
#define EXCHANGE_ID_BITFLYER 33
#define EXCHANGE_ID_KRAKEN 34
#define EXCHANGE_ID_IB     35
#define EXCHANGE_ID_BITTREX 36
#define EXCHANGE_ID_POLONIEX 37
#define EXCHANGE_ID_BITSTAMP 38
#define EXCHANGE_ID_DERIBIT 39
#define EXCHANGE_ID_EMX 40
#define EXCHANGE_ID_COINFLEX 41
#define EXCHANGE_ID_COINFLOOR 42
#define EXCHANGE_ID_MOCKKUCOIN 43
#define EXCHANGE_ID_MOCKBITMEX 44
#define EXCHANGE_ID_ERISX  45
#define EXCHANGE_ID_HBDM 46
#define EXCHANGE_ID_KUMEX  47
#define EXCHANGE_ID_BINANCEF 48
#define EXCHANGE_ID_BEQUANT 49
#define EXCHANGE_ID_COINCHECK 50
#define EXCHANGE_ID_KRAKENF 51
#define EXCHANGE_ID_OKEXF 52
#define EXCHANGE_ID_FTX 53
#define EXCHANGE_ID_MOCKHBDM = 53
#define EXCHANGE_ID_MOCKCOINFLEX = 54
#define EXCHANGE_ID_MOCKBINANCE = 55
#define EXCHANGE_ID_MOCKDERIBIT = 56
#define EXCHANGE_ID_GOOGLETRENDS 57
#define EXCHANGE_ID_QDP 58
#define EXCHANGE_ID_BINANCED 59

// MsgTypes that used for LF data structure...
const int MSG_TYPE_LF_ERRORMSG      = 15;
const int MSG_TYPE_LF_MD            = 101;
const int MSG_TYPE_LF_L2_MD         = 102;
const int MSG_TYPE_LF_L2_INDEX      = 103;
const int MSG_TYPE_LF_L2_ORDER      = 104;
const int MSG_TYPE_LF_L2_TRADE      = 105;
const int MSG_TYPE_LF_PRICE_BOOK_20 = 106;
const int MSG_TYPE_LF_PRICE_BOOK_REST = 107;
const int MSG_TYPE_LF_BAR_MD        = 110;
const int MSG_TYPE_LF_FUNDING       = 111;
const int MSG_TYPE_LF_PRICE_INDEX   = 112;
const int MSG_TYPE_LF_MARKPRICE     = 113;
const int MSG_TYPE_LF_PERPETUAL     = 114;
const int MSG_TYPE_LF_TICKER        = 115;
const int MSG_TYPE_LF_TRENDS_DATA   = 117;
const int MSG_TYPE_LF_QUOTE_REQUESTS = 120;

const int MSG_TYPE_LF_QRY_POS       = 201;
const int MSG_TYPE_LF_RSP_POS       = 202;
const int MSG_TYPE_LF_ORDER         = 204;
const int MSG_TYPE_LF_RTN_ORDER     = 205;
const int MSG_TYPE_LF_RTN_TRADE     = 206;
const int MSG_TYPE_LF_ORDER_ACTION  = 207;
const int MSG_TYPE_LF_QRY_ACCOUNT   = 208;
const int MSG_TYPE_LF_RSP_ACCOUNT   = 209;
const int MSG_TYPE_LF_WITHDRAW      = 210;
const int MSG_TYPE_LF_INNER_TRANSFER  = 211;
const int MSG_TYPE_LF_TRANSFER_HISTORY = 212;
const int MSG_TYPE_LF_QUOTE = 214;
const int MSG_TYPE_LF_RTN_QUOTE = 215;
const int MSG_TYPE_LF_QUOTE_ACTION = 216;
const int MSG_TYPE_LF_BATCH_CANCEL_ORDER = 217;
const int MSG_TYPE_LF_GET_KLINE_VIA_REST = 218;
const int MSG_TYPE_LF_BAR_SERIAL1000     = 219;

// MsgTypes that from original data structures...
// ctp, idx=1
const int MSG_TYPE_LF_MD_CTP        = 1101; // CThostFtdcDepthMarketDataField from ctp/ThostFtdcUserApiStruct.h
const int MSG_TYPE_LF_QRY_POS_CTP   = 1201; // CThostFtdcQryInvestorPositionField from ctp/ThostFtdcUserApiStruct.h
const int MSG_TYPE_LF_RSP_POS_CTP   = 1202; // CThostFtdcInvestorPositionField from ctp/ThostFtdcUserApiStruct.h
const int MSG_TYPE_LF_ORDER_CTP     = 1204; // CThostFtdcInputOrderField from ctp/ThostFtdcUserApiStruct.h
const int MSG_TYPE_LF_RTN_ORDER_CTP = 1205; // CThostFtdcOrderField from ctp/ThostFtdcUserApiStruct.h
const int MSG_TYPE_LF_RTN_TRADE_CTP = 1206; // CThostFtdcTradeField from ctp/ThostFtdcUserApiStruct.h
const int MSG_TYPE_LF_ORDER_ACTION_CTP = 1207; // CThostFtdcInputOrderActionField from ctp/ThostFtdcUserApiStruct.h
const int MSG_TYPE_LF_QRY_ACCOUNT_CTP = 1208; // CThostFtdcQryTradingAccountField from ctp/ThostFtdcUserApiStruct.h
const int MSG_TYPE_LF_RSP_ACCOUNT_CTP = 1209; // CThostFtdcTradingAccountField from ctp/ThostFtdcUserApiStruct.h

// xtp, idx=15
const int MSG_TYPE_LF_MD_XTP        = 15101; // XTPMarketDataStruct from xtp/xquote_api_struct.h
const int MSG_TYPE_LF_RSP_POS_XTP   = 15202; // XTPQueryStkPositionRsp from xtp/xoms_api_struct.h
const int MSG_TYPE_LF_ORDER_XTP     = 15204; // XTPOrderInsertInfo from xtp/xoms_api_struct.h
const int MSG_TYPE_LF_RTN_ORDER_XTP = 15205; // XTPOrderInfo from xtp/xoms_api_struct.h
const int MSG_TYPE_LF_RTN_TRADE_XTP = 15206; // XTPTradeReport from xtp/xoms_api_struct.h

//binance, idx=16
const int MSG_TYPE_LF_MD_BINANCE        = 16101;
const int MSG_TYPE_LF_QRY_POS_BINANCE   = 16201;
const int MSG_TYPE_LF_RSP_POS_BINANCE   = 16202;
const int MSG_TYPE_LF_ORDER_BINANCE     = 16204; 
const int MSG_TYPE_LF_RTN_ORDER_BINANCE = 16205; 
const int MSG_TYPE_LF_RTN_TRADE_BINANCE = 16206; 
const int MSG_TYPE_LF_ORDER_ACTION_BINANCE = 16207;
const int MSG_TYPE_LF_WITHDRAW_BINANCE  = 16210; 
const int MSG_TYPE_LF_INNER_TRANSFER_BINANCE  = 16211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_BINANCE = 16212;
const int MSG_TYPE_LF_GET_KLINE_VIA_REST_BINANCE = 16218;
const int MSG_TYPE_LF_BAR_SERIAL1000_BINANCE     = 16219;

//indodax, idx=17
const int MSG_TYPE_LF_MD_INDODAX        = 17101; 
const int MSG_TYPE_LF_ORDER_INDODAX    = 17204; 
const int MSG_TYPE_LF_RTN_ORDER_INDODAX = 17205; 
const int MSG_TYPE_LF_RTN_TRADE_INDODAX = 17206; 
const int MSG_TYPE_LF_ORDER_ACTION_INDODAX = 17207; 

//okex, idx=18
/*const int MSG_TYPE_LF_MD_OKEX        = 23101;
const int MSG_TYPE_LF_QRY_POS_OKEX   = 23201;
const int MSG_TYPE_LF_RSP_POS_OKEX   = 23202;
const int MSG_TYPE_LF_ORDER_OKEX     = 23204;
const int MSG_TYPE_LF_RTN_ORDER_OKEX = 23205;
const int MSG_TYPE_LF_RTN_TRADE_OKEX = 23206;
const int MSG_TYPE_LF_ORDER_ACTION_OKEX = 23207;*/

const int MSG_TYPE_LF_MD_OKEX        = 18101;
const int MSG_TYPE_LF_QRY_POS_OKEX   = 18201;
const int MSG_TYPE_LF_RSP_POS_OKEX   = 18202;
const int MSG_TYPE_LF_ORDER_OKEX     = 18204; 
const int MSG_TYPE_LF_RTN_ORDER_OKEX = 18205; 
const int MSG_TYPE_LF_RTN_TRADE_OKEX = 18206; 
const int MSG_TYPE_LF_ORDER_ACTION_OKEX = 18207;
const int MSG_TYPE_LF_WITHDRAW_OKEX  = 18210; 
const int MSG_TYPE_LF_INNER_TRANSFER_OKEX  = 18211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_OKEX = 18212;

//coinmex, idx=19
const int MSG_TYPE_LF_MD_COINMEX        = 19101;
const int MSG_TYPE_LF_QRY_POS_COINMEX   = 19201;
const int MSG_TYPE_LF_RSP_POS_COINMEX   = 19202;
const int MSG_TYPE_LF_ORDER_COINMEX     = 19204;
const int MSG_TYPE_LF_RTN_ORDER_COINMEX = 19205;
const int MSG_TYPE_LF_RTN_TRADE_COINMEX = 19206;
const int MSG_TYPE_LF_ORDER_ACTION_COINMEX = 19207;

//mock, idx=20
const int MSG_TYPE_LF_MD_MOCK        = 20101;
const int MSG_TYPE_LF_QRY_POS_MOCK   = 20201;
const int MSG_TYPE_LF_RSP_POS_MOCK   = 20202;
const int MSG_TYPE_LF_ORDER_MOCK     = 20204;
const int MSG_TYPE_LF_RTN_ORDER_MOCK = 20205;
const int MSG_TYPE_LF_RTN_TRADE_MOCK = 20206;
const int MSG_TYPE_LF_ORDER_ACTION_MOCK = 20207;

//bitmax, was rename ascendex , idx=21 
const int MSG_TYPE_LF_MD_ASCENDEX                 = 21101;
const int MSG_TYPE_LF_QRY_POS_ASCENDEX            = 21201;
const int MSG_TYPE_LF_RSP_POS_ASCENDEX            = 21202;
const int MSG_TYPE_LF_ORDER_ASCENDEX              = 21204;
const int MSG_TYPE_LF_RTN_ORDER_ASCENDEX          = 21205;
const int MSG_TYPE_LF_RTN_TRADE_ASCENDEX          = 21206;
const int MSG_TYPE_LF_ORDER_ACTION_ASCENDEX       = 21207;
const int MSG_TYPE_LF_WITHDRAW_ASCENDEX           = 21210;
const int MSG_TYPE_LF_INNER_TRANSFER_ASCENDEX     = 21211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_ASCENDEX   = 21212;
const int MSG_TYPE_LF_BATCH_CANCEL_ORDER_ASCENDEX = 21217;
const int MSG_TYPE_LF_GET_KLINE_VIA_REST_ASCENDEX = 21218;
const int MSG_TYPE_LF_BAR_SERIAL1000_ASCENDEX     = 21219;

//bitfinex, idx=22
const int MSG_TYPE_LF_MD_BITFINEX        = 22101;
const int MSG_TYPE_LF_QRY_POS_BITFINEX   = 22201;
const int MSG_TYPE_LF_RSP_POS_BITFINEX   = 22202;
const int MSG_TYPE_LF_ORDER_BITFINEX     = 22204;
const int MSG_TYPE_LF_RTN_ORDER_BITFINEX = 22205;
const int MSG_TYPE_LF_RTN_TRADE_BITFINEX = 22206;
const int MSG_TYPE_LF_ORDER_ACTION_BITFINEX = 22207;
const int MSG_TYPE_LF_WITHDRAW_BITFINEX  = 22210; 
const int MSG_TYPE_LF_INNER_TRANSFER_BITFINEX  = 22211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_BITFINEX = 22212;

//bitmex, idx=23
const int MSG_TYPE_LF_MD_BITMEX        = 23101;
const int MSG_TYPE_LF_QRY_POS_BITMEX   = 23201;
const int MSG_TYPE_LF_RSP_POS_BITMEX   = 23202;
const int MSG_TYPE_LF_ORDER_BITMEX     = 23204;
const int MSG_TYPE_LF_RTN_ORDER_BITMEX = 23205;
const int MSG_TYPE_LF_RTN_TRADE_BITMEX = 23206;
const int MSG_TYPE_LF_ORDER_ACTION_BITMEX = 23207;

//HITBTC, idx=24
const int MSG_TYPE_LF_MD_HITBTC        = 24101;
const int MSG_TYPE_LF_QRY_POS_HITBTC   = 24201;
const int MSG_TYPE_LF_RSP_POS_HITBTC   = 24202;
const int MSG_TYPE_LF_ORDER_HITBTC     = 24204;
const int MSG_TYPE_LF_RTN_ORDER_HITBTC = 24205;
const int MSG_TYPE_LF_RTN_TRADE_HITBTC = 24206;
const int MSG_TYPE_LF_ORDER_ACTION_HITBTC = 24207;

//OCEANEX, idx=25
const int MSG_TYPE_LF_MD_OCEANEX        = 25101;
const int MSG_TYPE_LF_QRY_POS_OCEANEX   = 25201;
const int MSG_TYPE_LF_RSP_POS_OCEANEX   = 25202;
const int MSG_TYPE_LF_ORDER_OCEANEX     = 25204;
const int MSG_TYPE_LF_RTN_ORDER_OCEANEX = 25205;
const int MSG_TYPE_LF_RTN_TRADE_OCEANEX = 25206;
const int MSG_TYPE_LF_ORDER_ACTION_OCEANEX = 25207;

//HUOBI, idx=26
const int MSG_TYPE_LF_MD_HUOBI        = 26101;
const int MSG_TYPE_LF_QRY_POS_HUOBI   = 26201;
const int MSG_TYPE_LF_RSP_POS_HUOBI   = 26202;
const int MSG_TYPE_LF_ORDER_HUOBI     = 26204;
const int MSG_TYPE_LF_RTN_ORDER_HUOBI = 26205;
const int MSG_TYPE_LF_RTN_TRADE_HUOBI = 26206;
const int MSG_TYPE_LF_ORDER_ACTION_HUOBI = 26207;
const int MSG_TYPE_LF_WITHDRAW_HUOBI  = 26210; 
const int MSG_TYPE_LF_INNER_TRANSFER_HUOBI  = 26211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_HUOBI = 26212;
const int MSG_TYPE_LF_GET_KLINE_VIA_REST_HUOBI = 26218;
const int MSG_TYPE_LF_BAR_SERIAL1000_HUOBI     = 26219;

//OCEANEXB, idx=27
//const int MSG_TYPE_LF_MD_OCEANEXB        = 27101;
const int MSG_TYPE_LF_QRY_POS_OCEANEXB   = 27201;
const int MSG_TYPE_LF_RSP_POS_OCEANEXB   = 27202;
const int MSG_TYPE_LF_ORDER_OCEANEXB     = 27204;
const int MSG_TYPE_LF_RTN_ORDER_OCEANEXB = 27205;
const int MSG_TYPE_LF_RTN_TRADE_OCEANEXB = 27206;
const int MSG_TYPE_LF_ORDER_ACTION_OCEANEXB = 27207;
//PROBIT, idx=28
const int MSG_TYPE_LF_MD_PROBIT        	= 28101;
const int MSG_TYPE_LF_QRY_POS_PROBIT   	= 28201;
const int MSG_TYPE_LF_RSP_POS_PROBIT   	= 28202;
const int MSG_TYPE_LF_ORDER_PROBIT     	= 28204;
const int MSG_TYPE_LF_RTN_ORDER_PROBIT 	= 28205;
const int MSG_TYPE_LF_RTN_TRADE_PROBIT 	= 28206;
const int MSG_TYPE_LF_ORDER_ACTION_PROBIT = 28207;

//BITHUMB, idx=29
const int MSG_TYPE_LF_MD_BITHUMB             = 29101;
const int MSG_TYPE_LF_QRY_POS_BITHUMB           = 29201;
const int MSG_TYPE_LF_RSP_POS_BITHUMB           = 29202;
const int MSG_TYPE_LF_ORDER_BITHUMB             = 29204;
const int MSG_TYPE_LF_RTN_ORDER_BITHUMB         = 29205;
const int MSG_TYPE_LF_RTN_TRADE_BITHUMB         = 29206;
const int MSG_TYPE_LF_ORDER_ACTION_BITHUMB     = 29207;

//UPBIT, idx=30
const int MSG_TYPE_LF_MD_UPBIT             = 30101;
const int MSG_TYPE_LF_QRY_POS_UPBIT           = 30201;
const int MSG_TYPE_LF_RSP_POS_UPBIT           = 30202;
const int MSG_TYPE_LF_ORDER_UPBIT             = 30204;
const int MSG_TYPE_LF_RTN_ORDER_UPBIT         = 30205;
const int MSG_TYPE_LF_RTN_TRADE_UPBIT         = 30206;
const int MSG_TYPE_LF_ORDER_ACTION_UPBIT     = 30207;

//DAYBIT, idx=31
const int MSG_TYPE_LF_MD_DAYBIT             = 31101;
const int MSG_TYPE_LF_QRY_POS_DAYBIT           = 31201;
const int MSG_TYPE_LF_RSP_POS_DAYBIT           = 31202;
const int MSG_TYPE_LF_ORDER_DAYBIT             = 31204;
const int MSG_TYPE_LF_RTN_ORDER_DAYBIT         = 31205;
const int MSG_TYPE_LF_RTN_TRADE_DAYBIT         = 31206;
const int MSG_TYPE_LF_ORDER_ACTION_DAYBIT     = 31207;

//KUCOIN, idx=32
const int MSG_TYPE_LF_MD_KUCOIN             = 32101;
const int MSG_TYPE_LF_QRY_POS_KUCOIN           = 32201;
const int MSG_TYPE_LF_RSP_POS_KUCOIN           = 32202;
const int MSG_TYPE_LF_ORDER_KUCOIN             = 32204;
const int MSG_TYPE_LF_RTN_ORDER_KUCOIN         = 32205;
const int MSG_TYPE_LF_RTN_TRADE_KUCOIN         = 32206;
const int MSG_TYPE_LF_ORDER_ACTION_KUCOIN      = 32207;
const int MSG_TYPE_LF_WITHDRAW_KUCOIN          = 32210;
const int MSG_TYPE_LF_INNER_TRANSFER_KUCOIN    = 32211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_KUCOIN = 32212;

//BITFLYER, idx=33
const int MSG_TYPE_LF_MD_BITFLYER             = 33101;
const int MSG_TYPE_LF_QRY_POS_BITFLYER           = 33201;
const int MSG_TYPE_LF_RSP_POS_BITFLYER           = 33202;
const int MSG_TYPE_LF_ORDER_BITFLYER             = 33204;
const int MSG_TYPE_LF_RTN_ORDER_BITFLYER         = 33205;
const int MSG_TYPE_LF_RTN_TRADE_BITFLYER         = 33206;
const int MSG_TYPE_LF_ORDER_ACTION_BITFLYER      = 33207;

//KRAKEN, idx=34
const int MSG_TYPE_LF_MD_KRAKEN             = 34101;
const int MSG_TYPE_LF_QRY_POS_KRAKEN           = 34201;
const int MSG_TYPE_LF_RSP_POS_KRAKEN           = 34202;
const int MSG_TYPE_LF_ORDER_KRAKEN             = 34204;
const int MSG_TYPE_LF_RTN_ORDER_KRAKEN         = 34205;
const int MSG_TYPE_LF_RTN_TRADE_KRAKEN         = 34206;
const int MSG_TYPE_LF_ORDER_ACTION_KRAKEN      = 34207;

//IB, idx=35
const int MSG_TYPE_LF_MD_IB             = 35101;
const int MSG_TYPE_LF_QRY_POS_IB           = 35201;
const int MSG_TYPE_LF_RSP_POS_IB           = 35202;
const int MSG_TYPE_LF_ORDER_IB             = 35204;
const int MSG_TYPE_LF_RTN_ORDER_IB         = 35205;
const int MSG_TYPE_LF_RTN_TRADE_IB         = 35206;
const int MSG_TYPE_LF_ORDER_ACTION_IB      = 35207;

//BITTREX, idx=36
const int MSG_TYPE_LF_MD_BITTREX             = 36101;
const int MSG_TYPE_LF_QRY_POS_BITTREX           = 36201;
const int MSG_TYPE_LF_RSP_POS_BITTREX           = 36202;
const int MSG_TYPE_LF_ORDER_BITTREX             = 36204;
const int MSG_TYPE_LF_RTN_ORDER_BITTREX         = 36205;
const int MSG_TYPE_LF_RTN_TRADE_BITTREX         = 36206;
const int MSG_TYPE_LF_ORDER_ACTION_BITTREX      = 36207;
const int MSG_TYPE_LF_WITHDRAW_BITTREX          = 36210;
const int MSG_TYPE_LF_TRANSFER_HISTORY_BITTREX   = 36212;

//POLONIEX, idx=37
const int MSG_TYPE_LF_MD_POLONIEX             = 37101;
const int MSG_TYPE_LF_QRY_POS_POLONIEX           = 37201;
const int MSG_TYPE_LF_RSP_POS_POLONIEX           = 37202;
const int MSG_TYPE_LF_ORDER_POLONIEX             = 37204;
const int MSG_TYPE_LF_RTN_ORDER_POLONIEX         = 37205;
const int MSG_TYPE_LF_RTN_TRADE_POLONIEX         = 37206;
const int MSG_TYPE_LF_ORDER_ACTION_POLONIEX      = 37207;

//BITSTAMP, idx=38
const int MSG_TYPE_LF_MD_BITSTAMP             = 38101;
const int MSG_TYPE_LF_QRY_POS_BITSTAMP           = 38201;
const int MSG_TYPE_LF_RSP_POS_BITSTAMP           = 38202;
const int MSG_TYPE_LF_ORDER_BITSTAMP             = 38204;
const int MSG_TYPE_LF_RTN_ORDER_BITSTAMP         = 38205;
const int MSG_TYPE_LF_RTN_TRADE_BITSTAMP         = 38206;
const int MSG_TYPE_LF_ORDER_ACTION_BITSTAMP      = 38207;

//DERIBIT, idx=39
const int MSG_TYPE_LF_MD_DERIBIT             = 39101;
const int MSG_TYPE_LF_PRICE_INDEX_DERIBIT       = 39112;
const int MSG_TYPE_LF_MARKPRICE_DERIBIT      = 39113;
const int MSG_TYPE_LF_PERPETUAL_DERIBIT       = 39114;
const int MSG_TYPE_LF_TICKER_DERIBIT       = 39115;
const int MSG_TYPE_LF_QRY_POS_DERIBIT           = 39201;
const int MSG_TYPE_LF_RSP_POS_DERIBIT           = 39202;
const int MSG_TYPE_LF_ORDER_DERIBIT             = 39204;
const int MSG_TYPE_LF_RTN_ORDER_DERIBIT         = 39205;
const int MSG_TYPE_LF_RTN_TRADE_DERIBIT         = 39206;
const int MSG_TYPE_LF_ORDER_ACTION_DERIBIT      = 39207;

//EMX, idx=40
const int MSG_TYPE_LF_MD_EMX             = 40101;
const int MSG_TYPE_LF_QRY_POS_EMX           = 40201;
const int MSG_TYPE_LF_RSP_POS_EMX           = 40202;
const int MSG_TYPE_LF_ORDER_EMX             = 40204;
const int MSG_TYPE_LF_RTN_ORDER_EMX         = 40205;
const int MSG_TYPE_LF_RTN_TRADE_EMX         = 40206;
const int MSG_TYPE_LF_ORDER_ACTION_EMX      = 40207;

//COINFLEX, idx=41
const int MSG_TYPE_LF_MD_COINFLEX             = 41101;
const int MSG_TYPE_LF_QRY_POS_COINFLEX           = 41201;
const int MSG_TYPE_LF_RSP_POS_COINFLEX           = 41202;
const int MSG_TYPE_LF_ORDER_COINFLEX             = 41204;
const int MSG_TYPE_LF_RTN_ORDER_COINFLEX         = 41205;
const int MSG_TYPE_LF_RTN_TRADE_COINFLEX         = 41206;
const int MSG_TYPE_LF_ORDER_ACTION_COINFLEX      = 41207;
//COINFLOOR, idx=42
const int MSG_TYPE_LF_MD_COINFLOOR                = 42101;
const int MSG_TYPE_LF_QRY_POS_COINFLOOR           = 42201;
const int MSG_TYPE_LF_RSP_POS_COINFLOOR           = 42202;
const int MSG_TYPE_LF_ORDER_COINFLOOR             = 42204;
const int MSG_TYPE_LF_RTN_ORDER_COINFLOOR         = 42205;
const int MSG_TYPE_LF_RTN_TRADE_COINFLOOR         = 42206;
const int MSG_TYPE_LF_ORDER_ACTION_COINFLOOR      = 42207;

//MOCKKUCOIN, idx=43
const int MSG_TYPE_LF_MD_MOCKKUCOIN               = 43101;
const int MSG_TYPE_LF_QRY_POS_MOCKKUCOIN           = 43201;
const int MSG_TYPE_LF_RSP_POS_MOCKKUCOIN           = 43202;
const int MSG_TYPE_LF_ORDER_MOCKKUCOIN             = 43204;
const int MSG_TYPE_LF_RTN_ORDER_MOCKKUCOIN         = 43205;
const int MSG_TYPE_LF_RTN_TRADE_MOCKKUCOIN         = 43206;
const int MSG_TYPE_LF_ORDER_ACTION_MOCKKUCOIN      = 43207;

//MOCKBITMEX, idx=44
const int MSG_TYPE_LF_MD_MOCKBITMEX               = 44101;
const int MSG_TYPE_LF_QRY_POS_MOCKBITMEX           = 44201;
const int MSG_TYPE_LF_RSP_POS_MOCKBITMEX           = 44202;
const int MSG_TYPE_LF_ORDER_MOCKBITMEX             = 44204;
const int MSG_TYPE_LF_RTN_ORDER_MOCKBITMEX         = 44205;
const int MSG_TYPE_LF_RTN_TRADE_MOCKBITMEX         = 44206;
const int MSG_TYPE_LF_ORDER_ACTION_MOCKBITMEX      = 44207;


//ERISX, idx=45
const int MSG_TYPE_LF_MD_ERISX           = 45101;
const int MSG_TYPE_LF_QRY_POS_ERISX      = 45201;
const int MSG_TYPE_LF_RSP_POS_ERISX      = 45202;
const int MSG_TYPE_LF_ORDER_ERISX        = 45204;
const int MSG_TYPE_LF_RTN_ORDER_ERISX    = 45205;
const int MSG_TYPE_LF_RTN_TRADE_ERISX    = 45206;
const int MSG_TYPE_LF_ORDER_ACTION_ERISX = 45207;

//HBDM, idx=46
const int MSG_TYPE_LF_MD_HBDM                     = 46101;
const int MSG_TYPE_LF_QRY_POS_HBDM                = 46201;
const int MSG_TYPE_LF_RSP_POS_HBDM                = 46202;
const int MSG_TYPE_LF_ORDER_HBDM                  = 46204;
const int MSG_TYPE_LF_RTN_ORDER_HBDM              = 46205;
const int MSG_TYPE_LF_RTN_TRADE_HBDM              = 46206;
const int MSG_TYPE_LF_ORDER_ACTION_HBDM           = 46207;
const int MSG_TYPE_LF_GET_KLINE_VIA_REST_HBDM     = 46218;
const int MSG_TYPE_LF_BAR_SERIAL1000_HBDM         = 46219;

//KUMEX, idx=47
const int MSG_TYPE_LF_MD_KUMEX             = 47101;
const int MSG_TYPE_LF_QRY_POS_KUMEX           = 47201;
const int MSG_TYPE_LF_RSP_POS_KUMEX           = 47202;
const int MSG_TYPE_LF_ORDER_KUMEX             = 47204;
const int MSG_TYPE_LF_RTN_ORDER_KUMEX         = 47205;
const int MSG_TYPE_LF_RTN_TRADE_KUMEX         = 47206;
const int MSG_TYPE_LF_ORDER_ACTION_KUMEX      = 47207;

//BINANCEF, idx=48
const int MSG_TYPE_LF_MD_BINANCEF                  = 48101;
const int MSG_TYPE_LF_QRY_POS_BINANCEF             = 48201;
const int MSG_TYPE_LF_RSP_POS_BINANCEF             = 48202;
const int MSG_TYPE_LF_ORDER_BINANCEF               = 48204;
const int MSG_TYPE_LF_RTN_ORDER_BINANCEF           = 48205;
const int MSG_TYPE_LF_RTN_TRADE_BINANCEF           = 48206;
const int MSG_TYPE_LF_ORDER_ACTION_BINANCEF        = 48207;
const int MSG_TYPE_LF_WITHDRAW_BINANCEF            = 48210; 
const int MSG_TYPE_LF_BATCH_CANCEL_ORDER_BINANCEF  = 48217;
const int MSG_TYPE_LF_GET_KLINE_VIA_REST_BINANCEF  = 48218;
const int MSG_TYPE_LF_BAR_SERIAL1000_BINANCEF      = 48219;

//BEQUANT, idx=49
const int MSG_TYPE_LF_MD_BEQUANT        = 49101;
const int MSG_TYPE_LF_QRY_POS_BEQUANT   = 49201;
const int MSG_TYPE_LF_RSP_POS_BEQUANT   = 49202;
const int MSG_TYPE_LF_ORDER_BEQUANT     = 49204;
const int MSG_TYPE_LF_RTN_ORDER_BEQUANT = 49205;
const int MSG_TYPE_LF_RTN_TRADE_BEQUANT = 49206;
const int MSG_TYPE_LF_ORDER_ACTION_BEQUANT = 49207;
const int MSG_TYPE_LF_WITHDRAW_BEQUANT  = 49210;
const int MSG_TYPE_LF_INNER_TRANSFER_BEQUANT  = 49211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_BEQUANT = 49212;
//COINCHECK, idx=50
const int MSG_TYPE_LF_MD_COINCHECK                 = 50101;
const int MSG_TYPE_LF_QRY_POS_COINCHECK             = 50201;
const int MSG_TYPE_LF_RSP_POS_COINCHECK             = 50202;
const int MSG_TYPE_LF_ORDER_COINCHECK               = 50204;
const int MSG_TYPE_LF_RTN_ORDER_COINCHECK           = 50205;
const int MSG_TYPE_LF_RTN_TRADE_COINCHECK           = 50206;
const int MSG_TYPE_LF_ORDER_ACTION_COINCHECK        = 50207;
const int MSG_TYPE_LF_WITHDRAW_COINCHECK          = 50210; 
//KRAKENF, idx=51
const int MSG_TYPE_LF_MD_KRAKENF        = 51101;
const int MSG_TYPE_LF_QRY_POS_KRAKENF   = 51201;
const int MSG_TYPE_LF_RSP_POS_KRAKENF   = 51202;
const int MSG_TYPE_LF_ORDER_KRAKENF     = 51204;
const int MSG_TYPE_LF_RTN_ORDER_KRAKENF = 51205;
const int MSG_TYPE_LF_RTN_TRADE_KRAKENF = 51206;
const int MSG_TYPE_LF_ORDER_ACTION_KRAKENF = 51207;
//OKEXF, idx=52
const int MSG_TYPE_LF_MD_OKEXF             = 52101;
const int MSG_TYPE_LF_QRY_POS_OKEXF           = 52201;
const int MSG_TYPE_LF_RSP_POS_OKEXF           = 52202;
const int MSG_TYPE_LF_ORDER_OKEXF             = 52204;
const int MSG_TYPE_LF_RTN_ORDER_OKEXF         = 52205;
const int MSG_TYPE_LF_RTN_TRADE_OKEXF         = 52206;
const int MSG_TYPE_LF_ORDER_ACTION_OKEXF      = 52207;
const int MSG_TYPE_LF_WITHDRAW_OKEXF          = 52210;
const int MSG_TYPE_LF_INNER_TRANSFER_OKEXF    = 52211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_OKEXF = 52212;
//FTX, idx=53
const int MSG_TYPE_LF_MD_FTX             = 53101;
const int MSG_TYPE_LF_QRY_POS_FTX           = 53201;
const int MSG_TYPE_LF_RSP_POS_FTX           = 53202;
const int MSG_TYPE_LF_ORDER_FTX             = 53204;
const int MSG_TYPE_LF_RTN_ORDER_FTX         = 53205;
const int MSG_TYPE_LF_RTN_TRADE_FTX         = 53206;
const int MSG_TYPE_LF_ORDER_ACTION_FTX      = 53207;
const int MSG_TYPE_LF_WITHDRAW_FTX          = 53210;
const int MSG_TYPE_LF_INNER_TRANSFER_FTX    = 53211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_FTX = 53212;

const int MSG_TYPE_LF_MD_MOCKHBDM               = 153101;
const int MSG_TYPE_LF_QRY_POS_MOCKHBDM           = 153201;
const int MSG_TYPE_LF_RSP_POS_MOCKHBDM           = 153202;
const int MSG_TYPE_LF_ORDER_MOCKHBDM             = 153204;
const int MSG_TYPE_LF_RTN_ORDER_MOCKHBDM         = 153205;
const int MSG_TYPE_LF_RTN_TRADE_MOCKHBDM         = 153206;
const int MSG_TYPE_LF_ORDER_ACTION_MOCKHBDM      = 153207;

const int MSG_TYPE_LF_MD_MOCKDERIBIT               = 156101;
const int MSG_TYPE_LF_QRY_POS_MOCKDERIBIT           = 156201;
const int MSG_TYPE_LF_RSP_POS_MOCKDERIBIT           = 156202;
const int MSG_TYPE_LF_ORDER_MOCKDERIBIT             = 156204;
const int MSG_TYPE_LF_RTN_ORDER_MOCKDERIBIT         = 156205;
const int MSG_TYPE_LF_RTN_TRADE_MOCKDERIBIT         = 156206;
const int MSG_TYPE_LF_ORDER_ACTION_MOCKDERIBIT      = 156207;

const int MSG_TYPE_LF_MD_MOCKCOINFLEX               = 154101;
const int MSG_TYPE_LF_QRY_POS_MOCKCOINFLEX           = 154201;
const int MSG_TYPE_LF_RSP_POS_MOCKCOINFLEX           = 154202;
const int MSG_TYPE_LF_ORDER_MOCKCOINFLEX             = 154204;
const int MSG_TYPE_LF_RTN_ORDER_MOCKCOINFLEX         = 154205;
const int MSG_TYPE_LF_RTN_TRADE_MOCKCOINFLEX         = 154206;
const int MSG_TYPE_LF_ORDER_ACTION_MOCKCOINFLEX      = 154207;

const int MSG_TYPE_LF_MD_MOCKBINANCE               = 155101;
const int MSG_TYPE_LF_QRY_POS_MOCKBINANCE           = 155201;
const int MSG_TYPE_LF_RSP_POS_MOCKBINANCE           = 155202;
const int MSG_TYPE_LF_ORDER_MOCKBINANCE             = 155204;
const int MSG_TYPE_LF_RTN_ORDER_MOCKBINANCE         = 155205;
const int MSG_TYPE_LF_RTN_TRADE_MOCKBINANCE         = 155206;
const int MSG_TYPE_LF_ORDER_ACTION_MOCKBINANCE      = 155207;

//GOOGLETRENDS. idx=57
const int MSG_TYPE_LF_MD_GOOGLETRENDS = 57101;
const int MSG_TYPE_LF_TRENDS_DATA_GOOGLETRENDS = 57117;
//QDP, idx=58
const int MSG_TYPE_LF_MD_QDP = 58101;
const int MSG_TYPE_LF_QRY_POS_QDP = 58201;
const int MSG_TYPE_LF_RSP_POS_QDP = 58202;
const int MSG_TYPE_LF_ORDER_QDP = 58204;
const int MSG_TYPE_LF_RTN_ORDER_QDP = 58205;
const int MSG_TYPE_LF_RTN_TRADE_QDP = 58206;
const int MSG_TYPE_LF_ORDER_ACTION_QDP = 58207;
const int MSG_TYPE_LF_QRY_ACCOUNT_QDP = 58208;
const int MSG_TYPE_LF_RSP_ACCOUNT_QDP = 58209;
const int MSG_TYPE_LF_WITHDRAW_QDP = 58210;
const int MSG_TYPE_LF_INNER_TRANSFER_QDP = 58211;
const int MSG_TYPE_LF_TRANSFER_HISTORY_QDP = 58212;
//BINANCED, idx=59
const int MSG_TYPE_LF_MD_BINANCED = 59101;
const int MSG_TYPE_LF_QRY_POS_BINANCED = 59201;
const int MSG_TYPE_LF_RSP_POS_BINANCED = 59202;
const int MSG_TYPE_LF_ORDER_BINANCED = 59204;
const int MSG_TYPE_LF_RTN_ORDER_BINANCED = 59205;
const int MSG_TYPE_LF_RTN_TRADE_BINANCED = 59206;
const int MSG_TYPE_LF_ORDER_ACTION_BINANCED = 59207;
const int MSG_TYPE_LF_WITHDRAW_BINANCED = 59210;
const int MSG_TYPE_LF_BATCH_CANCEL_ORDER_BINANCED = 59217;

///////////////////////////////////
// LfActionFlagType: 报单操作标志
///////////////////////////////////
//删除
#define LF_CHAR_Delete          '0'
//挂起
#define LF_CHAR_Suspend         '1'
//激活
#define LF_CHAR_Active          '2'
//修改
#define LF_CHAR_Modify          '3'

typedef char LfActionFlagType;

///////////////////////////////////
// LfContingentConditionType: 触发条件
///////////////////////////////////
//立即
#define LF_CHAR_Immediately     '1'
//止损
#define LF_CHAR_Touch           '2'
//止赢
#define LF_CHAR_TouchProfit     '3'
//预埋单
#define LF_CHAR_ParkedOrder     '4'
//最新价大于条件价
#define LF_CHAR_LastPriceGreaterThanStopPrice '5'
//最新价大于等于条件价
#define LF_CHAR_LastPriceGreaterEqualStopPrice '6'
//最新价小于条件价
#define LF_CHAR_LastPriceLesserThanStopPrice '7'
//最新价小于等于条件价
#define LF_CHAR_LastPriceLesserEqualStopPrice '8'
//卖一价大于条件价
#define LF_CHAR_AskPriceGreaterThanStopPrice '9'
//卖一价大于等于条件价
#define LF_CHAR_AskPriceGreaterEqualStopPrice 'A'
//卖一价小于条件价
#define LF_CHAR_AskPriceLesserThanStopPrice 'B'
//卖一价小于等于条件价
#define LF_CHAR_AskPriceLesserEqualStopPrice 'C'
//买一价大于条件价
#define LF_CHAR_BidPriceGreaterThanStopPrice 'D'
//买一价大于等于条件价
#define LF_CHAR_BidPriceGreaterEqualStopPrice 'E'
//买一价小于条件价
#define LF_CHAR_BidPriceLesserThanStopPrice 'F'
//买一价小于等于条件价
#define LF_CHAR_BidPriceLesserEqualStopPrice 'H'

typedef char LfContingentConditionType;

///////////////////////////////////
// LfDirectionType: 买卖方向
///////////////////////////////////
//买
#define LF_CHAR_Buy             '0'
//卖
#define LF_CHAR_Sell            '1'

typedef char LfDirectionType;

///////////////////////////////////
// LfForceCloseReasonType: 强平原因
///////////////////////////////////
//非强平
#define LF_CHAR_NotForceClose   '0'
//资金不足
#define LF_CHAR_LackDeposit     '1'
//客户超仓
#define LF_CHAR_ClientOverPositionLimit '2'
//会员超仓
#define LF_CHAR_MemberOverPositionLimit '3'
//持仓非整数倍
#define LF_CHAR_NotMultiple     '4'
//违规
#define LF_CHAR_Violation       '5'
//其它
#define LF_CHAR_Other           '6'
//自然人临近交割
#define LF_CHAR_PersonDeliv     '7'

typedef char LfForceCloseReasonType;

///////////////////////////////////
// LfHedgeFlagType: 投机套保标志
///////////////////////////////////
//投机
#define LF_CHAR_Speculation     '1'
//套利
#define LF_CHAR_Argitrage       '2'
//套保
#define LF_CHAR_Hedge           '3'
//做市商(femas)
#define LF_CHAR_MarketMaker     '4'
//匹配所有的值(femas)
#define LF_CHAR_AllValue        '9'

typedef char LfHedgeFlagType;

///////////////////////////////////
// LfOffsetFlagType: 开平标志
///////////////////////////////////
//开仓
#define LF_CHAR_Open            '0'
//平仓
#define LF_CHAR_Close           '1'
//强平
#define LF_CHAR_ForceClose      '2'
//平今
#define LF_CHAR_CloseToday      '3'
//平昨
#define LF_CHAR_CloseYesterday  '4'
//强减
#define LF_CHAR_ForceOff        '5'
//本地强平
#define LF_CHAR_LocalForceClose '6'
//不分开平
#define LF_CHAR_Non             'N'

typedef char LfOffsetFlagType;

///////////////////////////////////
// LfOrderPriceTypeType: 报单价格条件
///////////////////////////////////
//任意价
#define LF_CHAR_AnyPrice        '1'
//限价
#define LF_CHAR_LimitPrice      '2'
//最优价
#define LF_CHAR_BestPrice       '3'
//隐藏限价
#define LF_CHAR_HideLimitPrice  '4'

typedef char LfOrderPriceTypeType;

///////////////////////////////////
// LfOrderStatusType: 报单状态
///////////////////////////////////
//全部成交（最终状态）
#define LF_CHAR_AllTraded       '0'
//部分成交还在队列中
#define LF_CHAR_PartTradedQueueing '1'
//部分成交不在队列中（部成部撤， 最终状态）
#define LF_CHAR_PartTradedNotQueueing '2'
//未成交还在队列中
#define LF_CHAR_NoTradeQueueing '3'
//未成交不在队列中（被拒绝，最终状态）
#define LF_CHAR_NoTradeNotQueueing '4'
//撤单
#define LF_CHAR_Canceled        '5'
//订单已报入交易所未应答
#define LF_CHAR_AcceptedNoReply '6'
//未知
#define LF_CHAR_Unknown         'a'
//尚未触发
#define LF_CHAR_NotTouched      'b'
//已触发
#define LF_CHAR_Touched         'c'
//废单错误（最终状态）
#define LF_CHAR_Error           'd'
//订单已写入
#define LF_CHAR_OrderInserted   'i'
//前置已接受
#define LF_CHAR_OrderAccepted   'j'

#define LF_CHAR_PendingCancel   'k'

typedef char LfOrderStatusType;

///////////////////////////////////
// LfPosiDirectionType: 持仓多空方向
///////////////////////////////////
//净
#define LF_CHAR_Net             '1'
//多头
#define LF_CHAR_Long            '2'
//空头
#define LF_CHAR_Short           '3'

typedef char LfPosiDirectionType;

///////////////////////////////////
// LfPositionDateType: 持仓日期
///////////////////////////////////
//今日持仓
#define LF_CHAR_Today           '1'
//历史持仓
#define LF_CHAR_History         '2'
//两种持仓
#define LF_CHAR_Both            '3'

typedef char LfPositionDateType;

///////////////////////////////////
// LfTimeConditionType: 有效期类型
///////////////////////////////////
//立即完成，否则撤销
#define LF_CHAR_IOC             '1'
//本节有效
#define LF_CHAR_GFS             '2'
//当日有效
#define LF_CHAR_GFD             '3'
//指定日期前有效
#define LF_CHAR_GTD             '4'
//撤销前有效
#define LF_CHAR_GTC             '5'
//集合竞价有效
#define LF_CHAR_GFA             '6'
//FAK或IOC(yisheng)
#define LF_CHAR_FAK             'A'
//FOK(yisheng)
#define LF_CHAR_FOK             'O'

typedef char LfTimeConditionType;

///////////////////////////////////
// LfVolumeConditionType: 成交量类型
///////////////////////////////////
//任何数量
#define LF_CHAR_AV              '1'
//最小数量
#define LF_CHAR_MV              '2'
//全部数量
#define LF_CHAR_CV              '3'

typedef char LfVolumeConditionType;

///////////////////////////////////
// LfYsHedgeFlagType: 易盛投机保值类型
///////////////////////////////////
//保值
#define LF_CHAR_YsB             'B'
//套利
#define LF_CHAR_YsL             'L'
//无
#define LF_CHAR_YsNon           'N'
//投机
#define LF_CHAR_YsT             'T'

typedef char LfYsHedgeFlagType;

///////////////////////////////////
// LfYsOrderStateType: 易盛委托状态类型
///////////////////////////////////
//终端提交
#define LF_CHAR_YsSubmit        '0'
//已受理
#define LF_CHAR_YsAccept        '1'
//策略待触发
#define LF_CHAR_YsTriggering    '2'
//交易所待触发
#define LF_CHAR_YsExctriggering '3'
//已排队
#define LF_CHAR_YsQueued        '4'
//部分成交
#define LF_CHAR_YsPartFinished  '5'
//完全成交
#define LF_CHAR_YsFinished      '6'
//待撤消(排队临时状态)
#define LF_CHAR_YsCanceling     '7'
//待修改(排队临时状态)
#define LF_CHAR_YsModifying     '8'
//完全撤单
#define LF_CHAR_YsCanceled      '9'
//已撤余单
#define LF_CHAR_YsLeftDeleted   'A'
//指令失败
#define LF_CHAR_YsFail          'B'
//策略删除
#define LF_CHAR_YsDeleted       'C'
//已挂起
#define LF_CHAR_YsSuppended     'D'
//到期删除
#define LF_CHAR_YsDeletedForExpire 'E'
//已生效——询价成功
#define LF_CHAR_YsEffect        'F'
//已申请——行权、弃权、套利等申请成功
#define LF_CHAR_YsApply         'G'

typedef char LfYsOrderStateType;

///////////////////////////////////
// LfYsOrderTypeType: 易盛委托类型
///////////////////////////////////
//市价
#define LF_CHAR_YsMarket        '1'
//限价
#define LF_CHAR_YsLimit         '2'

typedef char LfYsOrderTypeType;

///////////////////////////////////
// LfYsPositionEffectType: 易盛开平类型
///////////////////////////////////
//平仓
#define LF_CHAR_YsClose         'C'
//不分开平
#define LF_CHAR_YsNon           'N'
//开仓
#define LF_CHAR_YsOpen          'O'
//平当日
#define LF_CHAR_YsCloseToday    'T'

typedef char LfYsPositionEffectType;

///////////////////////////////////
// LfYsSideTypeType: 易盛买卖类型
///////////////////////////////////
//双边
#define LF_CHAR_YsAll           'A'
//买入
#define LF_CHAR_YsBuy           'B'
//无
#define LF_CHAR_YsNon           'N'
//卖出
#define LF_CHAR_YsSell          'S'

typedef char LfYsSideTypeType;

///////////////////////////////////
// LfYsTimeConditionType: 易盛委托有效类型
///////////////////////////////////
//当日有效
#define LF_CHAR_YsGFD           '0'
//撤销前有效
#define LF_CHAR_YsGTC           '1'
//指定日期前有效
#define LF_CHAR_YsGTD           '2'
//FAK或IOC
#define LF_CHAR_YsFAK           '3'
//FOK
#define LF_CHAR_YsFOK           '4'

typedef char LfYsTimeConditionType;

#endif

