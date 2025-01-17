//
// Created by bingchen on 8/17/18.
//

#include "PriceBook20Assembler.h"
#include <mutex>
#include <Poco/Checksum.h>
std::mutex mutex_price_map;
PriceBook20Assembler::PriceBook20Assembler() : m_level(20)
{
}

PriceBook20Assembler::~PriceBook20Assembler()
{
    clearPriceBook();
}

void PriceBook20Assembler::EraseAskPrice(std::string ticker, int64_t price, int64_t volume , int tem )
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    auto iter = tickerPriceMap.find(ticker);
    if(iter != tickerPriceMap.end()) {
        std::vector<PriceAndVolume>::iterator itr;
        std::vector<PriceAndVolume>* priceBooks = iter->second->asksPriceAndVolumes;
        int idx = -1;
        for(itr = priceBooks->begin(); itr != priceBooks->end(); itr++)
        {
            idx++;
            if(price == itr.base()->price) {
                itr.base()->volume = itr.base()->volume - volume;
                break;
            }
        }

        if(itr != priceBooks->end()) {
            //find the price
            if (idx < m_level) {
                iter->second->hasLevel20AskChanged = true;
            }
            if(itr.base()->volume == 0 || volume == tem)
                priceBooks->erase(itr);
        }
    }
}

void PriceBook20Assembler::EraseBidPrice(std::string ticker, int64_t price, int64_t volume ,int tem)
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    auto iter = tickerPriceMap.find(ticker);
    if(iter != tickerPriceMap.end()) {
        std::vector<PriceAndVolume>::iterator itr;
        std::vector<PriceAndVolume>* priceBooks = iter->second->bidsPriceAndVolumes;
        int idx = -1;
        for(itr = priceBooks->begin(); itr != priceBooks->end(); itr++)
        {
            idx++;
            if(price == itr.base()->price) {
                itr.base()->volume = itr.base()->volume - volume;
                break;
            }
        }

        if(itr != priceBooks->end()) {
            //find the price
            if (idx < m_level) {
                iter->second->hasLevel20BidChanged = true;
            }
            if(itr.base()->volume == 0 || volume == tem )
                priceBooks->erase(itr);
        }
    }
}

void PriceBook20Assembler::UpdateAskPrice(std::string ticker, int64_t price, uint64_t volume, int tem )
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    std::vector<PriceAndVolume>* asksPriceAndVolume = nullptr;
    PriceLevelBooks* priceLevelBook = nullptr;
    auto iter = tickerPriceMap.find(ticker);
    if(iter != tickerPriceMap.end()) {
        priceLevelBook = iter->second;
        asksPriceAndVolume = iter->second->asksPriceAndVolumes;
    } else {
        priceLevelBook = new PriceLevelBooks();
        priceLevelBook->bidsPriceAndVolumes = new std::vector<PriceAndVolume>();
        priceLevelBook->asksPriceAndVolumes = new std::vector<PriceAndVolume>();
        tickerPriceMap.insert(std::pair<std::string, PriceLevelBooks*>(ticker, priceLevelBook));
        asksPriceAndVolume = priceLevelBook->asksPriceAndVolumes;
    }

    std::vector<PriceAndVolume>::iterator itr;
    int idx = -1;
    for(itr = asksPriceAndVolume->begin(); itr != asksPriceAndVolume->end(); itr++)
    {
        idx++;
        if(price < itr.base()->price) {
            break;
        }
        //exist price replace volume
        if(price == itr.base()->price) {
            if(tem == 1)//新增
                itr.base()->volume += volume;
            if(tem == 2)//减少
            {
                itr.base()->volume = itr.base()->volume - volume ;
                if(itr.base()->volume == 0)
                  asksPriceAndVolume->erase(itr);  
            }
            if(tem == 0)//替换
                itr.base()->volume = volume;
            if (idx < m_level) {
                priceLevelBook->hasLevel20AskChanged = true;
            }
            return;
        }
    }

    PriceAndVolume pv;
    pv.price = price;
    pv.volume = volume;
    asksPriceAndVolume->insert(itr, pv);
    if (idx < m_level) {
        priceLevelBook->hasLevel20AskChanged = true;
    }
}


void PriceBook20Assembler::UpdateBidPrice(std::string ticker, int64_t price, uint64_t volume,int tem )
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    std::vector<PriceAndVolume>* bidsPriceAndVolume = nullptr;
    PriceLevelBooks* priceLevelBook = nullptr;
    auto iter = tickerPriceMap.find(ticker);
    if(iter != tickerPriceMap.end()) {
        priceLevelBook = iter->second;
        bidsPriceAndVolume = iter->second->bidsPriceAndVolumes;
    } else {
        priceLevelBook = new PriceLevelBooks();
        priceLevelBook->bidsPriceAndVolumes = new std::vector<PriceAndVolume>();
        priceLevelBook->asksPriceAndVolumes = new std::vector<PriceAndVolume>();
        tickerPriceMap.insert(std::pair<std::string, PriceLevelBooks*>(ticker, priceLevelBook));
        bidsPriceAndVolume = priceLevelBook->bidsPriceAndVolumes;
    }

    std::vector<PriceAndVolume>::iterator itr;
    int idx = -1;
    for(itr = bidsPriceAndVolume->begin(); itr != bidsPriceAndVolume->end(); itr++)
    {
        idx++;
        if(price > itr.base()->price) {
            break;
        }
        //exist price replace volume
        if(price == itr.base()->price) {
            if(tem == 1)
                itr.base()->volume += volume;
            if(tem == 2)
                itr.base()->volume = itr.base()->volume - volume;
            if(tem == 0)
                itr.base()->volume = volume;
            if (idx < m_level) {
                priceLevelBook->hasLevel20BidChanged = true;
            }
            return;
        }
    }

    PriceAndVolume pv;
    pv.price = price;
    pv.volume = volume;
    bidsPriceAndVolume->insert(itr, pv);
    if (idx < m_level) {
        priceLevelBook->hasLevel20BidChanged = true;
    }
}

int64_t PriceBook20Assembler::GetBestAskPrice(std::string ticker)
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    auto iter = tickerPriceMap.find(ticker);
    if(iter != tickerPriceMap.end()) {
        std::vector<PriceAndVolume>* asksPriceAndVolumes = iter->second->asksPriceAndVolumes;
        if(!asksPriceAndVolumes->empty())
        {
            PriceAndVolume& bestAsk = asksPriceAndVolumes->front();
            return bestAsk.price;
        }
    }

    return -1;
}

int64_t PriceBook20Assembler::GetBestBidPrice(std::string ticker)
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    auto iter = tickerPriceMap.find(ticker);
    if(iter != tickerPriceMap.end()) {
        std::vector<PriceAndVolume>* bidsPriceAndVolumes = iter->second->bidsPriceAndVolumes;
        if(!bidsPriceAndVolumes->empty())
        {
            PriceAndVolume& bestBid = bidsPriceAndVolumes->front();
            return bestBid.price;
        }
    }

    return -1;
}
std::string PriceBook20Assembler::generateBookStringToCheckSum(std::string ticker)
{
    std::string data = "";
    std::lock_guard<std::mutex> lck(mutex_price_map);
    auto iter = tickerPriceMap.find(ticker);
    if (iter != tickerPriceMap.end()) 
    {
        std::vector<PriceAndVolume>* askBook = iter->second->asksPriceAndVolumes;
        std::vector<PriceAndVolume>* bidBook = iter->second->bidsPriceAndVolumes;
        while((nullptr != askBook && !askBook->empty())|| (nullptr != bidBook && !bidBook->empty()))
        {
            if(nullptr != bidBook && !bidBook->empty())
            {
                //data += ()
            }
            if(nullptr != askBook && !askBook->empty())
            {

            }
        }
     
    }
    return data;
}
bool PriceBook20Assembler::Assembler(std::string ticker, LFPriceBook20Field &md,int checksum)
{
    
    return false;
}

bool PriceBook20Assembler::Assembler(std::string ticker, LFPriceBook20Field &md)
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    std::vector<PriceAndVolume>* asksPriceAndVolume = nullptr;
    std::vector<PriceAndVolume>* bidsPriceAndVolume = nullptr;
    auto iter = tickerPriceMap.find(ticker);
    if(iter != tickerPriceMap.end()) {
        asksPriceAndVolume = iter->second->asksPriceAndVolumes;
        bidsPriceAndVolume = iter->second->bidsPriceAndVolumes;
        if(!iter->second->hasLevel20AskChanged && !iter->second->hasLevel20BidChanged) {
            return false;
        }
    } else {
        return false;
    }

    int askTotalSize = asksPriceAndVolume->size();
    auto size = std::min(askTotalSize, m_level);

    for(int i = 0; i < size; ++i)
    {
        md.AskLevels[i].price = asksPriceAndVolume->at(i).price;
        md.AskLevels[i].volume = asksPriceAndVolume->at(i).volume;
        //std::cout << "LFPriceBook20Field AskLevels: (i)" << i << "(price)" << md.AskLevels[i].price<<  "  (volume)"<< md.AskLevels[i].volume << std::endl;
    }
    md.AskLevelCount = size;

    int bidTotalSize = bidsPriceAndVolume->size();
    size = std::min(bidTotalSize, m_level);

    for(int i = 0; i < size; ++i)
    {
        md.BidLevels[i].price = bidsPriceAndVolume->at(i).price;
        md.BidLevels[i].volume = bidsPriceAndVolume->at(i).volume;
        //std::cout << "LFPriceBook20Field BidLevels: (i) " << i << "(price)" << md.BidLevels[i].price<<  "  (volume)"<< md.BidLevels[i].volume << std::endl;
    }
    md.BidLevelCount = size;

    strcpy(md.InstrumentID, ticker.c_str());
    iter->second->hasLevel20AskChanged = false;
    iter->second->hasLevel20BidChanged = false;

    return true;
}
//根据aggRate对同一区间对level进行合并
bool PriceBook20Assembler::Assembler(std::string ticker, LFPriceBook20Field &md,int64_t aggRate)
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    std::vector<PriceAndVolume>* asksPriceAndVolume = nullptr;
    std::vector<PriceAndVolume>* bidsPriceAndVolume = nullptr;
    auto iter = tickerPriceMap.find(ticker);
    if(iter != tickerPriceMap.end()) {
        asksPriceAndVolume = iter->second->asksPriceAndVolumes;
        bidsPriceAndVolume = iter->second->bidsPriceAndVolumes;
        if(!iter->second->hasLevel20AskChanged && !iter->second->hasLevel20BidChanged) {
            return false;
        }
    } else {
        return false;
    }

    int askTotalSize = asksPriceAndVolume->size();
    auto size = std::min(askTotalSize, m_level);
    int i = 0;
    int64_t last_price = 0;
    for(int j = 0; j < askTotalSize; ++j)
    {
        int64_t aggPrice = asksPriceAndVolume->at(j).price/aggRate;
        int64_t remain = asksPriceAndVolume->at(j).price%aggRate;
        if(remain > 0 )
        {//ask 向上合并 100.1->101
            aggPrice++;
        }
        if(last_price != aggPrice)
        {
            if (i >= size) break;
            md.AskLevels[i].price = aggPrice;
            md.AskLevels[i].volume = asksPriceAndVolume->at(j).volume;
            last_price = aggPrice;
            ++i;
        }
        else
        {
            md.AskLevels[i-1].volume += asksPriceAndVolume->at(j).volume;
        }
        //std::cout << "LFPriceBook20Field AskLevels: (i)" << i << "(price)" << md.AskLevels[i].price<<  "  (volume)"<< md.AskLevels[i].volume << std::endl;
    }
    md.AskLevelCount = i;

    int bidTotalSize = bidsPriceAndVolume->size();
    size = std::min(bidTotalSize, m_level);
    last_price = 0;
    i = 0;
    for(int j = 0; j < bidTotalSize;++j)
    {
        int64_t aggPrice = bidsPriceAndVolume->at(j).price/aggRate;
        if(last_price != aggPrice)
        {
            if (i >= size) break;
            md.BidLevels[i].price = aggPrice;
            md.BidLevels[i].volume = bidsPriceAndVolume->at(j).volume;
            last_price = aggPrice;
            ++i;
        }
        else
        {
            md.BidLevels[i-1].volume += bidsPriceAndVolume->at(j).volume;
        }
        
        //std::cout << "LFPriceBook20Field BidLevels: (i) " << i << "(price)" << md.BidLevels[i].price<<  "  (volume)"<< md.BidLevels[i].volume << std::endl;
    }
    md.BidLevelCount = i;

    strcpy(md.InstrumentID, ticker.c_str());
    iter->second->hasLevel20AskChanged = false;
    iter->second->hasLevel20BidChanged = false;

    return true;
}
void PriceBook20Assembler::clearPriceBook(std::string ticker)
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    auto iter = tickerPriceMap.find(ticker);
    if(iter != tickerPriceMap.end())
    {
        if (iter->second->asksPriceAndVolumes != nullptr) {
            iter->second->asksPriceAndVolumes->clear();
            delete iter->second->asksPriceAndVolumes;
        }

        if (iter->second->bidsPriceAndVolumes != nullptr) {
            iter->second->bidsPriceAndVolumes->clear();
            delete iter->second->bidsPriceAndVolumes;
        }
        delete iter->second;
        iter = tickerPriceMap.erase(iter);
    }
}

void PriceBook20Assembler::clearPriceBook()
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    auto iter = tickerPriceMap.begin();
    while(iter != tickerPriceMap.end())
    {
        if (iter->second->asksPriceAndVolumes != nullptr) {
            iter->second->asksPriceAndVolumes->clear();
            delete iter->second->asksPriceAndVolumes;
            iter->second->asksPriceAndVolumes = nullptr;
        }

        if (iter->second->bidsPriceAndVolumes != nullptr) {
            iter->second->bidsPriceAndVolumes->clear();
            delete iter->second->bidsPriceAndVolumes;
            iter->second->bidsPriceAndVolumes = nullptr;
        }
        delete iter->second;
        iter = tickerPriceMap.erase(iter);
    }
}

void PriceBook20Assembler::SetLevel(int level)
{
    if (level > 0 && level <= 20)
    {
        m_level = level;
    }
}

int PriceBook20Assembler::GetLevel()
{
    return m_level;
}
void PriceBook20Assembler::SetLeastLevel(int level)
{
    if (level > 0 && level <= GetLevel())
    {
        l_level = level;
    }
}
int PriceBook20Assembler::GetLeastLevel()
{
    return l_level;
}
int PriceBook20Assembler::GetNumberOfLevels_bids(std::string ticker)/*FXW's edits*/
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    auto iter = tickerPriceMap.find(ticker);
    int counts = 0;
    if (iter != tickerPriceMap.end()) {
        std::vector<PriceAndVolume>* priceBooks = iter->second->bidsPriceAndVolumes;
        return priceBooks->size();
    }
    return -1;
}
int PriceBook20Assembler::GetNumberOfLevels_asks(std::string ticker)/*FXW's edits*/
{
    std::lock_guard<std::mutex> lck(mutex_price_map);
    auto iter = tickerPriceMap.find(ticker);
    int counts = 0;
    if (iter != tickerPriceMap.end()) {
        std::vector<PriceAndVolume>* priceBooks = iter->second->asksPriceAndVolumes;
        return priceBooks->size();
    }
    return -1;
}
void PriceBook20Assembler::testPriceBook20Assembler() {

    //test clear
    PriceBook20Assembler priceBook20Assembler;
    std::string ticker = "BTCUSDT";
    priceBook20Assembler.EraseAskPrice(ticker, (int64_t)1);
    priceBook20Assembler.clearPriceBook();
    priceBook20Assembler.EraseBidPrice(ticker, (int64_t)1);
    priceBook20Assembler.clearPriceBook(ticker);
    priceBook20Assembler.clearPriceBook(ticker);
    priceBook20Assembler.clearPriceBook();


    LFPriceBook20Field md={0};
    //test data
    for(int i=0; i < 25; i++) {
        priceBook20Assembler.UpdateAskPrice(ticker, (int64_t) (i * 10), (uint64_t)99);
    }
    std::cout << "debug print " << std::endl;
    priceBook20Assembler.Assembler(ticker, md);

    //test insert
    priceBook20Assembler.UpdateAskPrice(ticker, (int64_t) 12, (uint64_t)99);
    std::cout << "debug print ,after 12 " << std::endl;
    priceBook20Assembler.Assembler(ticker, md);

    //test erase
    priceBook20Assembler.EraseAskPrice(ticker, (int64_t) 12);
    std::cout << "debug print " << std::endl;
    priceBook20Assembler.Assembler(ticker, md);


    //more then 20
    priceBook20Assembler.UpdateAskPrice(ticker, (int64_t) 191, (uint64_t)91);
    std::cout << "debug print ,after 21 " << std::endl;
    priceBook20Assembler.Assembler(ticker, md);


    priceBook20Assembler.UpdateAskPrice(ticker, (int64_t) 181, (uint64_t)81);
    std::cout << "debug print ,after 19 " << std::endl;
    priceBook20Assembler.Assembler(ticker, md);
    std::cout << "debug print again should no change " << std::endl;
    priceBook20Assembler.Assembler(ticker, md);

    //test update
    priceBook20Assembler.UpdateAskPrice(ticker, (int64_t) 170, (uint64_t)70);
    priceBook20Assembler.UpdateAskPrice(ticker, (int64_t) 190, (uint64_t)90);
    std::cout << "debug print ,after 190 " << std::endl;
    priceBook20Assembler.Assembler(ticker, md);

    std::cout << "done" << std::endl;
}

