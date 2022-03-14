/*****************************************************************************
 * Copyright [2017] [taurus.ai]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *****************************************************************************/

/**
 * IMDEngine: base class of all market data engine.
 * @Author cjiang (changhao.jiang@taurus.ai)
 * @since   April, 2017
 */

#include "IMDEngine.h"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/sys_messages.h" /**< for msg_type usage in listening */
#include <time.h>
USING_WC_NAMESPACE

IMDEngine::IMDEngine(int source): IEngine(source)
{
    subs_tickers.clear();
    subs_markets.clear();
    history_subs.clear();
}

void IMDEngine::set_reader_thread()
{
    reader_thread = ThreadPtr(new std::thread(boost::bind(&IMDEngine::listening, this)));
}

void IMDEngine::init()
{
    reader = yijinjing::JournalReader::createSysReader(name());
    JournalPair l1MdPair = getMdJournalPair(source_id);
    if (l1MdPair.first.empty() || l1MdPair.second.empty())
    {
        KF_LOG_INFO(logger, "getMdJournalPair failed ,{source_id:"<<  source_id << "}");
        return;
    }
    writer = yijinjing::JournalSafeWriter::create(l1MdPair.first, l1MdPair.second, name());
}

void IMDEngine::listening()
{
    yijinjing::FramePtr frame;
    while (isRunning && signal_received < 0)
    {
        frame = reader->getNextFrame();
        if (frame.get() != nullptr)
        {
            int msg_type = frame->getMsgType();
            int msg_source = frame->getSource();
            if (msg_source == source_id)
            {
                switch (msg_type)
                {
                    case MSG_TYPE_SUBSCRIBE_MARKET_DATA:
                    case MSG_TYPE_SUBSCRIBE_L2_MD:
                    case MSG_TYPE_SUBSCRIBE_INDEX:
                    case MSG_TYPE_SUBSCRIBE_ORDER_TRADE:
                    {
                        string ticker((char*)(frame->getData()));
                        size_t found = ticker.find(TICKER_MARKET_DELIMITER);
                        if (found != string::npos)
                        {
                            subs_tickers.push_back(ticker.substr(0, found));
                            subs_markets.push_back(ticker.substr(found + 1));
                            KF_LOG_DEBUG(logger, "[sub] (ticker)" << ticker.substr(0, found) << " (market)" << ticker.substr(found + 1));
                        }
                        else
                        {
                            subs_tickers.push_back(ticker);
                            subs_markets.push_back("");
                            KF_LOG_DEBUG(logger, "[sub] (ticker)" << ticker << " (market)null");
                        }

                        // maintain sub_counts
                        SubCountMap& sub_counts = history_subs[msg_type];
                        if (sub_counts.find(ticker) == sub_counts.end())
                        {
                            sub_counts[ticker] = 1;
                        }
                        else
                        {
                            sub_counts[ticker] += 1;
                        }

                        if (frame->getLastFlag() == 1)
                        {
                            if (is_logged_in())
                            {
                                if (msg_type == MSG_TYPE_SUBSCRIBE_MARKET_DATA)
                                {
                                    subscribeMarketData(subs_tickers, subs_markets);
                                }
                                else if (msg_type == MSG_TYPE_SUBSCRIBE_L2_MD)
                                {
                                    subscribeL2MD(subs_tickers, subs_markets);
                                }
                                else if (msg_type == MSG_TYPE_SUBSCRIBE_INDEX)
                                {
                                    subscribeIndex(subs_tickers, subs_markets);
                                }
                                else if (msg_type == MSG_TYPE_SUBSCRIBE_ORDER_TRADE)
                                {
                                    subscribeOrderTrade(subs_tickers, subs_markets);
                                }
                                subs_tickers.clear();
                                subs_markets.clear();
                            }
                        }
                    }
                }
            }

            if (msg_type == MSG_TYPE_MD_ENGINE_OPEN && (msg_source <= 0 || msg_source == source_id))
            {
                on_engine_open();
            }
            else if (msg_type == MSG_TYPE_MD_ENGINE_CLOSE && (msg_source <= 0 || msg_source == source_id))
            {
                on_engine_close();
            }
            else if (msg_type == MSG_TYPE_STRING_COMMAND)
            {
                string cmd((char*)frame->getData());
                on_command(cmd);
            }
        }
    }

    if (IEngine::signal_received >= 0)
    {
        KF_LOG_INFO(logger, "[IEngine] signal received: " << IEngine::signal_received);
    }

    if (!isRunning)
    {
        KF_LOG_INFO(logger, "[IEngine] forced to stop.");
    }
}

void IMDEngine::pre_run()
{
    subscribeHistorySubs();
}

void IMDEngine::subscribeHistorySubs()
{
    for (auto& iter: history_subs)
    {
        if (iter.second.size() == 0)
            continue;

        int msg_type = iter.first;

        vector<string> tickers;
        vector<string> markets;

        for (auto& tickerIter: iter.second)
        {
            const string& ticker = tickerIter.first;
            size_t found = ticker.find(TICKER_MARKET_DELIMITER);
            if (found != string::npos)
            {
                tickers.push_back(ticker.substr(0, found));
                markets.push_back(ticker.substr(found + 1));
            }
            else
            {
                tickers.push_back(ticker);
                markets.push_back("");
            }
        }
        if (msg_type == MSG_TYPE_SUBSCRIBE_MARKET_DATA)
        {
            subscribeMarketData(tickers, markets);
        }
        else if (msg_type == MSG_TYPE_SUBSCRIBE_L2_MD)
        {
            subscribeL2MD(tickers, markets);
        }
        else if (msg_type == MSG_TYPE_SUBSCRIBE_INDEX)
        {
            subscribeIndex(tickers, markets);
        }
        else if (msg_type == MSG_TYPE_SUBSCRIBE_ORDER_TRADE)
        {
            subscribeOrderTrade(tickers, markets);
        }
        KF_LOG_INFO(logger, "[sub] history (msg_type)" << msg_type << " (num)" << tickers.size());
    }
}

void IMDEngine::write_errormsg(int errorid,string errormsg)
{
    LFErrorMsgField error;
    strcpy(error.Name,get_module_name().data());
    strcpy(error.Type,"md");
    error.ErrorId = errorid;
    strcpy(error.ErrorMsg,errormsg.data());
    LFErrorMsgField* errorPtr = &error;
    KF_LOG_DEBUG(logger, "MD write_errormsg :"<< errormsg);
    writer->write_frame(errorPtr, sizeof(LFErrorMsgField), source_id, MSG_TYPE_LF_ERRORMSG, 1/*islast*/, -1/*invalidRid*/);
    //KF_LOG_DEBUG(logger, "MD write end");
}

void IMDEngine::on_market_data(const LFMarketDataField* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFMarketDataField), source_id, MSG_TYPE_LF_MD, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "%-10s[%ld, %lu | %ld, %lu]",
                         data->InstrumentID,
                         data->BidPrice1,
                         data->BidVolume1,
                         data->AskPrice1,
                         data->AskVolume1);
    }
}

void IMDEngine::on_market_bar_data(const LFBarMarketDataField* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFBarMarketDataField), source_id, MSG_TYPE_LF_BAR_MD, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "%-10s [open %ld, close %ld | low %ld, high %ld] [best bid %ld, best ask %ld][status %d]",/*quest3 editd by fxw*/
                         data->InstrumentID,
                         data->Open,
                         data->Close,
                         data->Low,
                         data->High,
                         data->BestBidPrice,
                         data->BestAskPrice);/*quest3 editd by fxw*/
    }
}

void IMDEngine::on_price_book_update(const LFPriceBook20Field* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFPriceBook20Field), source_id, MSG_TYPE_LF_PRICE_BOOK_20, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "price book 20 update: %-10s %d | %d [%ld, %lu | %ld, %lu] %d",/*FXW's edits*/
                         data->InstrumentID,
                         data->BidLevelCount,
                         data->AskLevelCount,
                         data->BidLevels[0].price,
                         data->BidLevels[0].volume,
                         data->AskLevels[0].price,
                         data->AskLevels[0].volume,
                         data->Status);
    }
}
void IMDEngine::on_price_book_update_from_rest(const LFPriceBook20Field* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFPriceBook20Field), source_id, MSG_TYPE_LF_PRICE_BOOK_REST, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "on_price_book_update_from_rest: %-10s %d | %d [%ld, %lu | %ld, %lu] %d",
                         data->InstrumentID,
                         data->BidLevelCount,
                         data->AskLevelCount,
                         data->BidLevels[0].price,
                         data->BidLevels[0].volume,
                         data->AskLevels[0].price,
                         data->AskLevels[0].volume,
                         data->Status);
    }
}
void IMDEngine::on_trade(const LFL2TradeField* trade)
{
    if (isRunning)
    {
        writer->write_frame(trade, sizeof(LFL2TradeField), source_id, MSG_TYPE_LF_L2_TRADE, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "%-10s [%ld, %lu][%d]",/*quest3 edited by fxw*/
                         trade->InstrumentID,
                         trade->Price,
                         trade->Volume,
                         trade->Status); /*quest3 edited by fxw*/
    }
}

void IMDEngine::on_funding_update(const LFFundingField* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFFundingField), source_id, MSG_TYPE_LF_FUNDING, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "funding data update: %-10s %s | %lld, %lld, %lf, %lf]",
                         data->InstrumentID,
                         data->ExchangeID,
                         data->TimeStamp,
                         data->Interval,
                         data->Rate,
                         data->RateDaily);
    }
}

void IMDEngine::on_quote_requests(const LFQuoteRequestsField* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFQuoteRequestsField), source_id, MSG_TYPE_LF_QUOTE_REQUESTS, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "quote_requests data update: %-10s %s | %ld, %lu]",
                         data->InstrumentID,
                         data->ExchangeID,
                         data->Price,
                         data->Volume);
    }
}

void IMDEngine::on_priceindex(const LFPriceIndex* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFPriceIndex), source_id, MSG_TYPE_LF_PRICE_INDEX, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "priceindex data: InstrumentID %s TimeStamp %s Price %11f]",
                         data->InstrumentID,
                         data->TimeStamp,
                         data->Price);
    }
}


void IMDEngine::on_markprice(const LFMarkPrice* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFMarkPrice), source_id, MSG_TYPE_LF_MARKPRICE, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "markprice data: InstrumentID %s Iv %11f MarkPrice %11f]",
                         data->InstrumentID,
                         data->Iv,
                         data->MarkPrice);
    }
}

void IMDEngine::on_perpetual(const LFPerpetual* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFPerpetual), source_id, MSG_TYPE_LF_PERPETUAL, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "perpetual data: InstrumentID %s Interest %11f]",
                         data->InstrumentID,
                         data->Interest);
    }
}

void IMDEngine::on_ticker(const LFTicker* data)
{
    if (isRunning)
    {
        writer->write_frame(data, sizeof(LFTicker), source_id, MSG_TYPE_LF_TICKER, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "TICKER data: InstrumentID %s Ask_iv %11f Best_ask_amount %11f Best_ask_price %11f Best_bid_amount %11f Best_bid_price %11f Bid_iv %11f Mark_price %11f Last_price %11f Open_interest %11f Underlying_price %11f Delta %11f Vega %11f Volume24 %11f]",
                         data->InstrumentID,
                         data->Ask_iv,
                         data->Best_ask_amount,
                         data->Best_ask_price,
                         data->Best_bid_amount,
                         data->Best_bid_price,
                         data->Bid_iv,
                         data->Mark_price,
                         data->Last_price,
                         data->Open_interest,
                         data->Underlying_price,
                         data->Delta,                                                                                                    
                         data->Vega,
                         data->Volume24);
    }
}

void IMDEngine::on_trends_data(const GoogleTrendsData* data)
{
    KF_LOG_INFO(logger, "MDEngineGoogletrends::on_trends_data");
    if (isRunning)
    {
        writer->write_frame(data, sizeof(GoogleTrendsData), source_id, MSG_TYPE_LF_TRENDS_DATA, 1/*islast*/, -1/*invalidRid*/);
        KF_LOG_DEBUG_FMT(logger, "%-10s[%s, %s, %d]",
            data->KeyWord,
            data->CountryOrRegion,
            data->FormattedTime,
            data->Value);
    }
}

std::string IMDEngine::timestamp_to_formatISO8601(int64_t timestamp)
{
    int ms = timestamp % 1000;
    tm utc_time{};
    time_t time = timestamp/1000;
    gmtime_r(&time, &utc_time);
    char timeStr[50]{};
    sprintf(timeStr, "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", utc_time.tm_year + 1900, utc_time.tm_mon + 1, utc_time.tm_mday, utc_time.tm_hour, utc_time.tm_min, utc_time.tm_sec,ms);
    return std::string(timeStr);
}
int64_t IMDEngine::formatISO8601_to_timestamp(std::string time)
{
    //extern long timezone;  
    int year,month,day,hour,min,sec,millsec;
    sscanf(time.c_str(),"%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",&year,&month,&day,&hour,&min,&sec,&millsec);
    tm utc_time{};
    utc_time.tm_year = year - 1900;
    utc_time.tm_mon = month -1;
    utc_time.tm_mday = day;
    utc_time.tm_hour = hour;
    utc_time.tm_min = min;
    utc_time.tm_sec = sec;
    time_t timet = mktime(&utc_time);
    tzset();
    KF_LOG_DEBUG_FMT(logger, "formatISO8601_to_timestamp timezone:%lld",timezone);
    return (timet-timezone)*1000+millsec;
}
int64_t IMDEngine::formatISO8601_to_timestamp_ns(std::string time)
{
    //extern long timezone;  
    int year,month,day,hour,min,sec,nanosec;
    sscanf(time.c_str(),"%04d-%02d-%02dT%02d:%02d:%02d.%09dZ",&year,&month,&day,&hour,&min,&sec,&nanosec);
    tm utc_time{};
    utc_time.tm_year = year - 1900;
    utc_time.tm_mon = month -1;
    utc_time.tm_mday = day;
    utc_time.tm_hour = hour;
    utc_time.tm_min = min;
    utc_time.tm_sec = sec;
    time_t timet = mktime(&utc_time);
    tzset();
    KF_LOG_DEBUG_FMT(logger, "formatISO8601_to_timestamp_ns timezone:%lld",timezone);
    return (timet-timezone)*1000000000+nanosec;
}