#include "MDEngineMock.h"
#include "TypeConvert.hpp"
#include "Timer.h"
#include "longfist/LFUtils.h"
#include "longfist/LFDataStruct.h"
#include "IEngine.h"

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
#include <unistd.h>
#include <fstream>
#include <stdlib.h>

using std::string;
using std::to_string;
using std::stod;
using std::stoi;
using namespace std;


USING_WC_NAMESPACE

static MDEngineMock* global_md = nullptr;

std::vector <std::string> t_data;
std::vector <std::string> b_data;
std::vector <std::string> k_data;


json book_data_paths;
vector<string> book_paths;//the array of book data file path
ifstream *bookFiles;//the stream of book files
vector<string> bookDataLine;


json trade_data_paths;
vector<string> trade_paths;//the array of trade data file path
ifstream *tradeFiles;//the stream of trade files
vector<string> tradeDataLine;

bool hasKline = false;
json kline_data_paths;
vector<string> kline_paths;//the array of kline data file path
ifstream* klineFiles;//the stream of kline files
vector<string> klineDataLine;

struct session_data {
    int fd;
};

MDEngineMock::MDEngineMock(): IMDEngine(SOURCE_MOCK)
//MDEngineMock::MDEngineMock(): IMDEngine(SOURCE_MOCKBINANCE)
{
    logger = yijinjing::KfLog::getLogger("MdEngine.Mock");
}
MDEngineMock::~MDEngineMock()
{
	if(bookFiles)
		delete[] bookFiles;
}

void MDEngineMock::load(const json& j_config)//we should get the file name here and use them
{
	KF_LOG_INFO(logger, "MDEngineMock:: strat load  : ");

	book_data_paths=j_config["book_data_path"];
	playback_rate = j_config["playback_rate"].get<int64_t>();
	buffer_line_size = j_config["buffer_line_size"].get<int>();
	bookFiles = new ifstream[book_data_paths.size()];
	if(!bookFiles) 
	{
		KF_LOG_ERROR(logger, "MDEngineMock::[load] can't get memory ");
	}
//	memset(book_data_path,0,sizeof(bookFiles));
	int count=0;//count of file

	for(auto& book_data_path:book_data_paths)//get all file name 
	{
		string book_path=book_data_path.get<string>();
		book_paths.push_back(book_path);
		KF_LOG_INFO(logger, "MDEngineMock:: strat get file  : "<<book_path<< "  NO:"<<count);
		//if(!bookFiles) 
		//KF_LOG_INFO(logger, "MDEngineMock:: test point : "<<bookFiles<<" " << "  NO:"<<count);

		//KF_LOG_INFO(logger, "MDEngineMock:: test point : "<<bookFiles[0]<< "  NO:"<<count);
		
		bookFiles[count].open(book_path.c_str(),ios::binary);
		if(!bookFiles[count].is_open())
		{
			KF_LOG_ERROR(logger, "MDEngineMock::[load] can't find file "<<book_path.c_str());
			return;
		}
		count++;
	}
	
	KF_LOG_INFO(logger, "MDEngineMock:: read book file : ");
	for(int i=0;i<count;i++)//read one line firse
	{
		string line;
		getline(bookFiles[i],line);
		for(int j=0;j<buffer_line_size;j++)
		{
		    if(!getline(bookFiles[i],line))  
			{
				bookFiles[i].close();
				break; // 每次缓存 buffer_line_size
			}
			get_book_map(line,i);
		    //bookDataLine.push_back(line);
		}
	}

    /*
	for(int i=0;i<bookDataLine.size();i++)
	{
		if(bookDataLine[i] !="")
		{
			get_book_map(bookDataLine[i],i);	
		}
	}
	*/

	KF_LOG_INFO(logger, "MDEngineMock:: strat load trade  : ");

	trade_data_paths=j_config["trade_data_path"];
	tradeFiles = new ifstream[trade_data_paths.size()];
	if(!tradeFiles) 
	{
		KF_LOG_ERROR(logger, "MDEngineMock::[load] can't get memory ");
	}
	count=0;//count of file

	for(auto& trade_data_path:trade_data_paths)//get all file name 
	{
		
		string trade_path=trade_data_path.get<string>();
		trade_paths.push_back(trade_path);
		KF_LOG_INFO(logger, "MDEngineMock:: strat get file  : "<<trade_path<< "  NO:"<<count);
 
		KF_LOG_INFO(logger, "MDEngineMock:: test point : "<<tradeFiles<<" " << "  NO:"<<count);

		KF_LOG_INFO(logger, "MDEngineMock:: test point : "<<tradeFiles[0]<< "  NO:"<<count);
		
		tradeFiles[count].open(trade_path.c_str(),ios::binary);
		if(!tradeFiles[count].is_open())
		{
			KF_LOG_ERROR(logger, "MDEngineMock::[load] can't find file "<<trade_path.c_str());
			return;
		}
		count++;

	}
	
	KF_LOG_INFO(logger, "MDEngineMock:: read trade file : ");
	for(int i=0;i<count;i++)//read one line firse
	{
		string line;
		getline(tradeFiles[i],line);
		for(int j=0;j<buffer_line_size;j++)
		{
			KF_LOG_INFO(logger, "MDEngineMock::  line: "<< line);
		    if(!getline(tradeFiles[i],line)) 
			{
				tradeFiles[i].close();
				break;
			}
            get_trade_map(line,i);	
		    //tradeDataLine.push_back(line);
		}
	}
    /*
	for(int i=0;i<tradeDataLine.size();i++)
	{
		if(tradeDataLine[i] !="")
		{
			
		}
	}*/

	//add kline
	if (j_config.find("kline_data_path") != j_config.end() && j_config["kline_data_path"].size() != 0) {
		KF_LOG_INFO(logger, "MDEngineMock:: strat load kline  : ");
		hasKline = true;

		kline_data_paths = j_config["kline_data_path"];
		klineFiles = new ifstream[kline_data_paths.size()];
		if (!klineFiles)
		{
			KF_LOG_ERROR(logger, "MDEngineMock::[load](kline) can't get memory ");
		}
		count = 0;//count of file
		for (auto& kline_data_path : kline_data_paths)//get all file name 
		{
			string kline_path = kline_data_path.get<string>();
			kline_paths.push_back(kline_path);
			KF_LOG_INFO(logger, "MDEngineMock:: strat get file  : " << kline_path << "  NO:" << count);
			KF_LOG_INFO(logger, "MDEngineMock:: test point : " << klineFiles << " " << "  NO:" << count);
			KF_LOG_INFO(logger, "MDEngineMock:: test point : " << klineFiles[0] << "  NO:" << count);
			klineFiles[count].open(kline_path.c_str(), ios::binary);
			if (!klineFiles[count].is_open())
			{
				KF_LOG_ERROR(logger, "MDEngineMock::[load] can't find file " << kline_path.c_str());
				return;
			}
			count++;
		}

		KF_LOG_INFO(logger, "MDEngineMock:: read kline file : ");
		for (int i = 0; i < count; i++)//read one line firse
		{
			string line;
			getline(klineFiles[i], line);
			for (int j = 0; j < buffer_line_size; j++)
			{
				KF_LOG_INFO(logger, "MDEngineMock::  line: " << line);
				if (!getline(klineFiles[i], line))
				{
					klineFiles[i].close();
					break;
				}
				get_kline_map(line, i);
			}
		}
		if (kline_line_data.empty()) kline_data_firtm = -1;
		else kline_data_firtm = kline_line_data.begin()->first;
	}
	//add kline end

    if(book_line_data.empty()) book_data_firtm = -1;
	else book_data_firtm = book_line_data.begin()->first;
	if(trade_line_data.empty()) trade_data_firtm = -1;
   	else trade_data_firtm = trade_line_data.begin()->first;

    readWhiteLists(j_config);
}

void MDEngineMock::readWhiteLists(const json& j_config)
{
	KF_LOG_INFO(logger, "[readWhiteLists]");

	if(j_config.find("whiteLists") != j_config.end()) {
		KF_LOG_INFO(logger, "[readWhiteLists] found whiteLists");
		//has whiteLists
		json whiteLists = j_config["whiteLists"].get<json>();
		if(whiteLists.is_object())
		{
			for (json::iterator it = whiteLists.begin(); it != whiteLists.end(); ++it) {
				std::string strategy_coinpair = it.key();
				std::string exchange_coinpair = it.value();
				KF_LOG_INFO(logger, "[readWhiteLists] (strategy_coinpair) " << strategy_coinpair << " (exchange_coinpair) " << exchange_coinpair);
				keyIsStrategyCoinpairWhiteList.insert(std::pair<std::string, std::string>(strategy_coinpair, exchange_coinpair));				
			}
		}
	}
}

std::vector<std::string> MDEngineMock::split(std::string str, std::string token)
{
	std::vector<std::string>result;
	while (str.size()) {
		size_t index = str.find(token);
		if (index != std::string::npos) {
			result.push_back(str.substr(0, index));
			str = str.substr(index + token.size());
			if (str.size() == 0)result.push_back(str);
		}
		else {
			result.push_back(str);
			str = "";
		}
	}
	return result;
}


void MDEngineMock::connect(long timeout_nsec)
{
    KF_LOG_INFO(logger, "MDEngineMock::connect:");
    connected = true;
}

void MDEngineMock::login(long timeout_nsec)
{
	KF_LOG_INFO(logger, "MDEngineMock::login:");
	logged_in = true;
}

void MDEngineMock::set_reader_thread()
{
	IMDEngine::set_reader_thread();

	//modified here
	if (!hasKline) {
		KF_LOG_INFO(logger, "[set_reader_thread] compare_thread start on MDEngineMock::compare_loop");
		compare_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineMock::compare_loop, this)));
	}
	else {
		KF_LOG_INFO(logger, "[set_reader_thread] compare_thread start on MDEngineMock::compare_loop_with_kline");
		compare_thread = ThreadPtr(new std::thread(boost::bind(&MDEngineMock::compare_loop_with_kline, this)));
	}
	
	//KF_LOG_INFO(logger, "[set_reader_thread] book_thread start on MDEngineMock::book_loop");
    //book_thread    = ThreadPtr(new std::thread(boost::bind(&MDEngineMock::book_loop, this)));

    //KF_LOG_INFO(logger, "[set_reader_thread] trade_thread start on MDEngineMock::trade_loop");
    //trade_thread   = ThreadPtr(new std::thread(boost::bind(&MDEngineMock::trade_loop, this)));
	//modified end
}

void MDEngineMock::logout()
{
   KF_LOG_INFO(logger, "MDEngineMock::logout:");
}

void MDEngineMock::release_api()
{
   KF_LOG_INFO(logger, "MDEngineMock::release_api:");
}

//modified begin
void MDEngineMock::compare_loop()
{
	KF_LOG_INFO(logger, "compare_loop start");
	int64_t sleeping_time;
	int64_t book_data_pre = book_data_firtm;
	int64_t book_data_now = -1;
	int64_t trade_data_pre = trade_data_firtm;
	int64_t trade_data_now = -1;

	bool book_empty = false;
	bool trade_empty = false;
	//先判断是否都为空 或者进行初始化
	if (book_data_pre == -1)
		book_empty = true;
	else
		book_data_now = book_line_data.begin()->first;
	if (trade_data_pre == -1)
		trade_empty = true;
	else
		trade_data_now = trade_line_data.begin()->first;

	while (isRunning)
	{
		//都不为空
		if (!book_empty && !trade_empty)
		{
			// 读的下一条数据的时间 book比trade早
			if (book_data_now <= trade_data_now) 
			{
				KF_LOG_INFO(logger, "[compare_loop] compare (onDepth)");
				if (!onDepth())
				{
					book_empty = true;
					continue; // end of data
				}
				// 计算等待时间
				book_data_pre = book_data_now;
				book_data_now = book_line_data.begin()->first;
				sleeping_time = ((book_data_now <= trade_data_now) ? (book_data_now - book_data_pre) : (trade_data_now - book_data_pre)) / playback_rate;
				KF_LOG_INFO(logger, "book_data_now = " << book_data_now << "，trade_data_now = " << trade_data_now << "，book_data_pre = " << book_data_pre << ", sleeping_time = " << sleeping_time);
			}
			// 读的下一条trade比book早
			else {
				KF_LOG_INFO(logger, "[compare_loop] compare (onFills) ");
				if (!onFills())
				{
					trade_empty = true;
					continue; // end of data
				}
				// 计算等待时间
				trade_data_pre = trade_data_now;
				trade_data_now = trade_line_data.begin()->first;
				sleeping_time = ((trade_data_now <= book_data_now) ? (trade_data_now - trade_data_pre) : (book_data_now - trade_data_pre)) / playback_rate;
				KF_LOG_INFO(logger, "compare book_data_now = " << book_data_now << "，trade_data_now = " << trade_data_pre << "，book_data_pre = " << trade_data_pre << ", sleeping_time = " << sleeping_time);
			}
			// 等
			if (sleeping_time > 150000)
				this_thread::sleep_for(chrono::nanoseconds(sleeping_time)); // 每book_sleeping_time时间存一条数据
		}

		// 只有trade为空
		else if (!book_empty && trade_empty) {
			KF_LOG_INFO(logger, "[compare_loop] (onDepth) ");
			if (!onDepth())
				break; // end of data
			// 计算等待时间
			book_data_now = book_line_data.begin()->first; //第二条的时间
			sleeping_time = (book_data_now - book_data_pre) / playback_rate;
			KF_LOG_INFO(logger, "delta time = " << (book_data_now - book_data_pre) << ",sleeping_time = " << sleeping_time);
			book_data_pre = book_data_now;
			if (sleeping_time > 150000)
				this_thread::sleep_for(chrono::nanoseconds(sleeping_time)); // 每book_sleeping_time时间存一条数据
		}
		// 只有book为空
		else if (book_empty && !trade_empty) {
			KF_LOG_INFO(logger, "[compare_loop] (onFills) ");
			if (!onFills())
				break; // end of data
			// 计算等待时间
			trade_data_now = trade_line_data.begin()->first;
			sleeping_time = (trade_data_now - trade_data_pre) / playback_rate;
			KF_LOG_INFO(logger, "delta time = " << (trade_data_now - trade_data_pre) << ",sleeping_time = " << sleeping_time);
			trade_data_pre = trade_data_now;
			if (sleeping_time > 150000)
				this_thread::sleep_for(chrono::nanoseconds(sleeping_time));
		}
		// trade book都为空，退出
		else if (book_empty && trade_empty) {
			KF_LOG_INFO(logger, "[compare_loop] book and trade is empty ");
			break;
		}
	}
}

//0 for book(105), 1 for trade(106), 2 for kline(110)
//book_empty  trade_empty  kline_empty中最多只有一个为真
int MDEngineMock::compare_data_time(int64_t book_time, int64_t trade_time, int64_t kline_time,
	bool book_empty, bool trade_empty, bool kline_empty)
{
	if (!book_empty && !trade_empty && !kline_empty) {
		if (book_time < trade_time && book_time < kline_time)
			return 0;
		else 
			return (trade_time < kline_time)? 1 : 2;
	}
	else if(book_empty)
		return (trade_time < kline_time) ? 1 : 2;
	else if(trade_empty)
		return (book_time < kline_time)  ? 0 : 2;
	else if (kline_empty)
		return (book_time < trade_time)  ? 0 : 1;
}

int64_t MDEngineMock::calculate_wait_time(int64_t pre, int64_t book_time, int64_t trade_time, int64_t kline_time,
	bool book_empty, bool trade_empty, bool kline_empty)
{
	switch (compare_data_time(book_time, trade_time, kline_time, book_empty, trade_empty, kline_empty)) {
	case 0:
		return book_time - pre;
		break;
	case 1:
		return trade_time - pre;
		break;
	case 2:
		return kline_time - pre;
		break;
	}
}

void MDEngineMock::compare_loop_with_kline()
{
	KF_LOG_INFO(logger, "compare_loop_with_kline start");
	int64_t sleeping_time;
	int64_t book_data_pre = book_data_firtm;
	int64_t book_data_now = -1;
	int64_t trade_data_pre = trade_data_firtm;
	int64_t trade_data_now = -1;
	int64_t kline_data_pre = kline_data_firtm;
	int64_t kline_data_now = -1;

	bool book_empty = false;
	bool trade_empty = false;
	bool kline_empty = false;
	//先判断是否都为空 或者进行初始化
	if (book_data_pre == -1)
		book_empty = true;
	else
		book_data_now = book_line_data.begin()->first;
	if (trade_data_pre == -1)
		trade_empty = true;
	else
		trade_data_now = trade_line_data.begin()->first;
	if (kline_data_pre == -1)
		kline_empty = true;
	else
		kline_data_now = kline_line_data.begin()->first;

	while (isRunning)
	{
		//都不为空
		//if (!book_empty && !trade_empty && !kline_empty)
		//其中两个不为空
		if ((!book_empty && !trade_empty) || (!book_empty && !kline_empty)|| (!kline_empty && !trade_empty))
		{
			int cp_flag = compare_data_time(book_data_now, trade_data_now, kline_data_now, book_empty, trade_empty, kline_empty);

			// 读的下一条数据的时间 book最早
			if (cp_flag == 0)
			{
				KF_LOG_INFO(logger, "[compare_loop] compare (onDepth)");
				if (!onDepth())
				{
					book_empty = true;
					continue; // end of data
				}
				// 计算等待时间 暂时不认为book_empty会为false
				book_data_pre = book_data_now;
				book_data_now = book_line_data.begin()->first;
				sleeping_time = (calculate_wait_time(book_data_pre, book_data_now, trade_data_now, kline_data_now, false, trade_empty, kline_empty)) / playback_rate;
				KF_LOG_INFO(logger, "(book_data_now) " << book_data_now << "，(trade_data_now) " << trade_data_now << "，(kline_data_now) " << kline_data_now
					<< "，(book_data_pre) " << book_data_pre << ", (sleeping_time) " << sleeping_time);
			}
			// 读的下一条数据的时间 trade最早
			else if(cp_flag == 1) {
				KF_LOG_INFO(logger, "[compare_loop] compare (onFills) ");
				if (!onFills())
				{
					trade_empty = true;
					continue; // end of data
				}
				// 计算等待时间 暂时不认为trade_empty会为false
				trade_data_pre = trade_data_now;
				trade_data_now = trade_line_data.begin()->first;
				sleeping_time = (calculate_wait_time(trade_data_pre, book_data_now, trade_data_now, kline_data_now, book_empty, false, kline_empty)) / playback_rate;
				KF_LOG_INFO(logger, "(book_data_now) " << book_data_now << "，(trade_data_now) " << trade_data_now << "，(kline_data_now) " << kline_data_now
					<< "，(trade_data_pre) " << trade_data_pre << ", (sleeping_time) " << sleeping_time);
			}
			else if (cp_flag == 2) {
				KF_LOG_INFO(logger, "[compare_loop] compare (onKline) ");
				if (!onKline())
				{
					kline_empty = true;
					continue; // end of data
				}
				// 计算等待时间 暂时不认为kline_empty会为false
				kline_data_pre = kline_data_now;
				kline_data_now = kline_line_data.begin()->first;
				sleeping_time = (calculate_wait_time(kline_data_pre, book_data_now, trade_data_now, kline_data_now, book_empty, trade_empty, false)) / playback_rate;
				KF_LOG_INFO(logger, "(book_data_now) " << book_data_now << "，(trade_data_now) " << trade_data_now << "，(kline_data_now) " << kline_data_now
					<< "，(kline_data_pre) " << kline_data_pre << ", (sleeping_time) " << sleeping_time);
			}
			// 等
			if (sleeping_time > 150000)
				this_thread::sleep_for(chrono::nanoseconds(sleeping_time)); // 每book_sleeping_time时间存一条数据
		}

		// book不为空且trade kline都为空
		else if (!book_empty && trade_empty && kline_empty) {
			KF_LOG_INFO(logger, "[compare_loop] (onDepth) ");
			if (!onDepth())
				break; // end of data
			// 计算等待时间
			book_data_now = book_line_data.begin()->first; //第二条的时间
			sleeping_time = (book_data_now - book_data_pre) / playback_rate;
			KF_LOG_INFO(logger, "delta time = " << (book_data_now - book_data_pre) << ",sleeping_time = " << sleeping_time);
			book_data_pre = book_data_now;
			if (sleeping_time > 150000)
				this_thread::sleep_for(chrono::nanoseconds(sleeping_time)); // 每book_sleeping_time时间存一条数据
		}
		// trade不为空且book kline都为空
		else if (book_empty && !trade_empty && kline_empty) {
			KF_LOG_INFO(logger, "[compare_loop] (onFills) ");
			if (!onFills())
				break; // end of data
			// 计算等待时间
			trade_data_now = trade_line_data.begin()->first;
			sleeping_time = (trade_data_now - trade_data_pre) / playback_rate;
			KF_LOG_INFO(logger, "delta time = " << (trade_data_now - trade_data_pre) << ",sleeping_time = " << sleeping_time);
			trade_data_pre = trade_data_now;
			if (sleeping_time > 150000)
				this_thread::sleep_for(chrono::nanoseconds(sleeping_time));
		}
		// kline不为空且trade book都为空
		else if (book_empty && trade_empty && !kline_empty) {
			KF_LOG_INFO(logger, "[compare_loop] (onKline) ");
			if (!onKline())
				break; // end of data
			// 计算等待时间
			kline_data_now = kline_line_data.begin()->first;
			sleeping_time = (kline_data_now - kline_data_pre) / playback_rate;
			KF_LOG_INFO(logger, "delta time = " << (kline_data_now - kline_data_pre) << ",sleeping_time = " << sleeping_time);
			kline_data_pre = kline_data_now;
			if (sleeping_time > 150000)
				this_thread::sleep_for(chrono::nanoseconds(sleeping_time));
		}

		// trade book kline都为空，退出
		else if (book_empty && trade_empty && kline_empty) {
			KF_LOG_INFO(logger, "[compare_loop] book and trade is empty ");
			break;
		}
	}
}
//modified end

void MDEngineMock::get_trade_map(string info,int idex)
{
	KF_LOG_DEBUG_FMT(logger,"MDEngineMock:: info %s",info.c_str());
	std::vector<string> result = split(info, ",");
	TDdata t;
	strcpy(t.TD.InstrumentID, result[2].c_str());
	strcpy(t.TD.ExchangeID, result[1].c_str());
	t.TD.Price = std::stod((result[3]).c_str()) ;
	t.TD.Volume = std::stod((result[4]).c_str()) ;

	t.TD.OrderBSFlag[0] = result[6][0];
	t.TD.Status = atoi(result[11].c_str());
	//modified TimeStamp and TradeTime
	strcpy(t.TD.TradeTime, result[0].c_str());

	int64_t nano = stoll(result[19]);
	t.source_id = (short)atoi(result[22].c_str());
	trade_line_data.insert(std::make_pair(nano,std::make_pair(t,idex)));
	auto it_trade = trade_line_data.begin();

	KF_LOG_DEBUG_FMT(logger,"MDEngineMock::get_trade_map:  debug [ %lld , [ info, %d ] ]",it_trade->first,(it_trade->second).second);

}
bool MDEngineMock::onFills()
{
    static size_t trade_index = 0;
	if(trade_line_data.empty()) return false;
    auto it_trade = trade_line_data.begin();
	int64_t nano = it_trade->first;
	auto& info_map=it_trade->second;

	auto& trade_info = info_map.first;
	int minpos=info_map.second;

	LFL2TradeField trade;
	memset(&trade, 0, sizeof(trade));
	strcpy(trade.InstrumentID, trade_info.TD.InstrumentID);
	strcpy(trade.ExchangeID, trade_info.TD.ExchangeID);

	trade.Price = trade_info.TD.Price ;
	trade.Volume = trade_info.TD.Volume ;

	trade.OrderBSFlag[0] = trade_info.TD.OrderBSFlag[0];
	trade.Status = trade_info.TD.Status;

	//modified TimeStamp and TradeTime
	trade.TimeStamp = nano;
	strcpy(trade.TradeTime, trade_info.TD.TradeTime);
	//KF_LOG_INFO(logger, "onFills, TradeTime: " << trade.TimeStamp);

	//KF_LOG_INFO(logger, "MDEngineMock::[onFills] (ticker)" << trade.InstrumentID <<" (Price)" << trade.Price <<" (trade.Volume)" << trade.Volume);
	short source_id = trade_info.source_id;
	writer->write_frame(&trade, sizeof(LFL2TradeField), source_id, MSG_TYPE_LF_L2_TRADE, 1/*islast*/, -1/*invalidRid*/);
	KF_LOG_DEBUG_FMT(logger, "on_l2_trade(%d,index:%ld):%s [%ld, %lu][%d]",source_id,trade_index,
							trade.InstrumentID,
							trade.Price,
							trade.Volume,
							trade.Status);
	KF_LOG_DEBUG_FMT(logger,"MDEngineMock::onFills: the result send is FFFFFFFFF%lld",nano);


	it_trade=trade_line_data.erase(it_trade);
	KF_LOG_DEBUG_FMT(logger,"MDEngineMock::onFills:the data size is %d",trade_line_data.size());


	trade_index++;
	string line;
	if(trade_line_data.size()<=1)
	{
		for(int j=0;j<buffer_line_size;j++)
		{
            if(!getline(tradeFiles[minpos],line))
	        {
		        tradeFiles[minpos].close();
		        break;
	        }
			//tradeDataLine.push_back(line);
	        get_trade_map(line,minpos);   		
		}
	}
    
    return true;
    
}

void MDEngineMock::get_book_map (string info,int idex) 
{
	KF_LOG_INFO(logger, "MDEngineMock:: info: "<<info.c_str());
	std::vector<string> result = split(info, ",");
	KF_LOG_INFO(logger,"nano "<<result[9].c_str());
	MDdata m;
	std::vector<string> bid = split(result[5], ";");
	std::vector<string> ask = split(result[6], ";");
	m.MD.BidLevelCount = atoi(result[3].c_str());
	m.MD.AskLevelCount = atoi(result[4].c_str());
	
	for (int j = 0; j < m.MD.BidLevelCount; ++j)
	{
		std::vector<string> pv = split(bid[j], "@");
		m.MD.BidLevels[j].price = atol(pv[1].c_str());
		m.MD.BidLevels[j].volume = strtoul(pv[0].c_str(),NULL,10);
	}
	for (int j = 0; j < m.MD.AskLevelCount; ++j)
	{
		std::vector<string> pv = split(ask[j], "@");
		m.MD.AskLevels[j].price = atol(pv[1].c_str());
		m.MD.AskLevels[j].volume = strtoul(pv[0].c_str(),NULL,10);
	}

	strcpy(m.MD.InstrumentID, result[0].c_str());
	strcpy(m.MD.ExchangeID, result[1].c_str());
	m.MD.Status = atoi(result[7].c_str());
	m.source_id = (short)atoi(result[12].c_str());
	//modified UpdateMicroSecond
	m.MD.UpdateMicroSecond = stoll(result[2]);

	int64_t nano = stoll(result[9]); 
	KF_LOG_INFO(logger,"nano2 "<<nano);
	book_line_data.insert(std::make_pair(nano, std::make_pair(m,idex)));
	auto it_book = book_line_data.begin();
	KF_LOG_DEBUG_FMT(logger,"MDEngineMock::get_book_map:  debug [ %lld , [ info, %d ] ]",it_book->first,(it_book->second).second);

}

bool MDEngineMock::onDepth()
{
	//book_data_path
		
		static int book_index=0;
		if(book_line_data.empty()) 
			return false;
		auto it_book = book_line_data.begin();
		
		int64_t nano = it_book->first;
		auto& info_map = it_book->second;

		auto& book_info = info_map.first;
		int minpos=info_map.second;
		KF_LOG_DEBUG_FMT(logger,"MDEngineMock::onDepth:  debug [ %lld , [ info, %d ] ]",nano,minpos);

		LFPriceBook20Field md;
		memset(&md, 0, sizeof(md));
        strcpy(md.InstrumentID, book_info.MD.InstrumentID);
        strcpy(md.ExchangeID, book_info.MD.ExchangeID);
        md.BidLevelCount = book_info.MD.BidLevelCount;
        md.AskLevelCount = book_info.MD.AskLevelCount;
		for (int j = 0; j < md.BidLevelCount; ++j)
		{
			md.BidLevels[j].price = book_info.MD.BidLevels[j].price;
			md.BidLevels[j].volume = book_info.MD.BidLevels[j].volume;
			//KF_LOG_INFO(logger, "MDEngineMock::onDepth:  LFPriceBook20Field BidLevels: (j) " << j << "(price)" << md.BidLevels[j].price << "  (volume)" << md.BidLevels[j].volume);
		}
        for (int j = 0; j < md.AskLevelCount; ++j)
		{
			md.AskLevels[j].price = book_info.MD.AskLevels[j].price;
			md.AskLevels[j].volume = book_info.MD.AskLevels[j].volume;
			//KF_LOG_INFO(logger, "MDEngineMock::onDepth:  LFPriceBook20Field AskLevels: (j) " << j << "(price)" << md.AskLevels[j].price << "  (volume)" << md.AskLevels[j].volume);
		}
		md.Status = book_info.MD.Status;

		KF_LOG_DEBUG_FMT(logger," debug the book idex is %d, the file number is %d, the nano is %lld",book_index,minpos,nano);
		//modified UpdateMicroSecond
		md.UpdateMicroSecond = book_info.MD.UpdateMicroSecond;
		//KF_LOG_INFO(logger, "onDepth, UpdateMicroSecond: " << md.UpdateMicroSecond);

        int source_id = book_info.source_id;
        KF_LOG_INFO(logger, "MDEngineMock::onDepth: on_price_book_update (ticker)" << md.InstrumentID);
		{
			writer->write_frame(&md, sizeof(LFPriceBook20Field), source_id, MSG_TYPE_LF_PRICE_BOOK_20, 1/*islast*/, -1/*invalidRid*/);
			KF_LOG_DEBUG_FMT(logger, "price book 20 update(%d,index:%ld): %-10s %d | %d [%ld, %lu | %ld, %lu] %d",source_id,book_index,
				md.InstrumentID,
				md.BidLevelCount,
				md.AskLevelCount,
				md.BidLevels[0].price,
				md.BidLevels[0].volume,
				md.AskLevels[0].price,
				md.AskLevels[0].volume,
				md.Status);
		}
		KF_LOG_DEBUG_FMT(logger,"MDEngineMock::onDepth: the result send is DDDDDDDDDD%lld",nano);


		it_book=book_line_data.erase(it_book);
		KF_LOG_DEBUG_FMT(logger,"MDEngineMock::onDepth:the data size is %d",book_line_data.size());


		book_index++;
		string line;
		if(book_line_data.size()<=1)
		{
			for(int j=0;j<buffer_line_size;j++)
			{

                if(!getline(bookFiles[minpos],line))
		        {
			        bookFiles[minpos].close();
			        break;
		        }
		        //bookDataLine.push_back(line);
		        get_book_map(line,minpos);
			}
		}
        return true;
}
// modified begin
void MDEngineMock::get_kline_map(string info, int idex) {
	KF_LOG_INFO(logger, "MDEngineMock:: info: " << info.c_str());
	std::vector<string> result = split(info, ",");
	KF_LOG_INFO(logger, "nano " << result[20].c_str());
	KLdata k;
	strcpy(k.KL.TradingDay, result[0].c_str());
	strcpy(k.KL.InstrumentID, result[1].c_str());
	strcpy(k.KL.ExchangeID, result[2].c_str());
	strcpy(k.KL.StartUpdateTime, result[5].c_str());
	strcpy(k.KL.EndUpdateTime, result[7].c_str());

	k.KL.UpperLimitPrice = atoll(result[3].c_str());
	k.KL.LowerLimitPrice = atoll(result[4].c_str());
	k.KL.StartUpdateMillisec = atoll(result[6].c_str());
	k.KL.EndUpdateMillisec = atoll(result[8].c_str());
	k.KL.PeriodMillisec = atoi(result[9].c_str());

	k.KL.Open  = atoll(result[10].c_str());
	k.KL.Close = atoll(result[11].c_str());
	k.KL.Low   = atoll(result[12].c_str());
	k.KL.High  = atoll(result[13].c_str());

	//k.KL.Volume = atoll(result[14].c_str());
	k.KL.Volume       = strtoull(result[14].c_str(), NULL, 10);
	//k.KL.StartVolume  = atoll(result[15].c_str());
	k.KL.StartVolume  = strtoull(result[15].c_str(), NULL, 10);
	k.KL.BestBidPrice = atoll(result[16].c_str());
	k.KL.BestAskPrice = atoll(result[17].c_str());
	//k.KL.CurrencyVolume = atoll(result[18].c_str());
	k.KL.CurrencyVolume = strtoull(result[18].c_str(), NULL, 10);

	k.source_id = (short)atoi(result[23].c_str());

	KF_LOG_DEBUG_FMT(logger, "get_kline_map: open, close, high, low [%ld, %ld, %ld, %ld]", k.KL.Open, k.KL.Close, k.KL.Low, k.KL.High);
	KF_LOG_DEBUG_FMT(logger, "get_kline_map string: open, close, high, low [%s, %s, %s, %s]", 
		result[10].c_str(),
		result[11].c_str(),
		result[12].c_str(),
		result[13].c_str());

	int64_t nano = stoll(result[20]);
	KF_LOG_INFO(logger, "nano3 " << nano);
	kline_line_data.insert(std::make_pair(nano, std::make_pair(k, idex)));
	auto it_kline = kline_line_data.begin();
	KF_LOG_DEBUG_FMT(logger, "MDEngineMock::get_kline_map:  debug [ %lld , [ info, %d ] ]", it_kline->first, (it_kline->second).second);
}

bool MDEngineMock::onKline()
{
	static int kline_index = 0;
	if (kline_line_data.empty())
		return false;
	auto it_kline = kline_line_data.begin();

	int64_t nano = it_kline->first;
	auto& info_map = it_kline->second;

	auto& kline_info = info_map.first;
	int minpos = info_map.second;
	KF_LOG_DEBUG_FMT(logger, "MDEngineMock::onKline:  debug [ %lld , [ info, %d ] ]", nano, minpos);

	LFBarMarketDataField kl;
	memset(&kl, 0, sizeof(kl));
	strcpy(kl.InstrumentID, kline_info.KL.InstrumentID);
	strcpy(kl.ExchangeID, kline_info.KL.ExchangeID);
	strcpy(kl.TradingDay, kline_info.KL.TradingDay);
	strcpy(kl.StartUpdateTime, kline_info.KL.StartUpdateTime);
	strcpy(kl.EndUpdateTime, kline_info.KL.EndUpdateTime);

	kl.UpperLimitPrice = kline_info.KL.UpperLimitPrice;
	kl.LowerLimitPrice = kline_info.KL.LowerLimitPrice;
	kl.StartUpdateMillisec = kline_info.KL.StartUpdateMillisec;
	kl.EndUpdateMillisec = kline_info.KL.EndUpdateMillisec;
	kl.PeriodMillisec = kline_info.KL.PeriodMillisec;

	kl.Open  = kline_info.KL.Open;
	kl.Close = kline_info.KL.Close;
	kl.Low   = kline_info.KL.Low;
	kl.High  = kline_info.KL.High;

	kl.Volume		  = kline_info.KL.Volume;
	kl.StartVolume    = kline_info.KL.StartVolume;
	kl.BestBidPrice   = kline_info.KL.BestBidPrice;
	kl.BestAskPrice   = kline_info.KL.BestAskPrice;
	kl.CurrencyVolume = kline_info.KL.CurrencyVolume;


	KF_LOG_DEBUG_FMT(logger, " debug the kline idex is %d, the file number is %d, the nano is %lld", kline_index, minpos, nano);
	//KF_LOG_INFO(logger, "onKline, UpdateMicroSecond: " << kl.UpdateMicroSecond);

	int source_id = kline_info.source_id;
	KF_LOG_INFO(logger, "MDEngineMock::onKline: on_market_bar_data (ticker)" << kl.InstrumentID);
	{
		writer->write_frame(&kl, sizeof(LFBarMarketDataField), source_id, MSG_TYPE_LF_BAR_MD, 1/*islast*/, -1/*invalidRid*/);
		KF_LOG_DEBUG_FMT(logger, "kline update(%d,index:%ld): %-10s | open, close, high, low [%ld, %ld, %ld, %ld]", source_id, kline_index,
			kl.InstrumentID,
			kl.Open,
			kl.Close,
			kl.High,
			kl.Low);
	}
	KF_LOG_DEBUG_FMT(logger, "MDEngineMock::onKline: the result send is DDDDDDDDDD%lld", nano);

	it_kline = kline_line_data.erase(it_kline);
	KF_LOG_DEBUG_FMT(logger, "MDEngineMock::onKline:the data size is %d", kline_line_data.size());


	kline_index++;
	string line;
	if (kline_line_data.size() <= 1)
	{
		for (int j = 0; j < buffer_line_size; j++)
		{

			if (!getline(klineFiles[minpos], line))
			{
				klineFiles[minpos].close();
				break;
			}
			//klineDataLine.push_back(line);
			get_kline_map(line, minpos);
		}
	}
	return true;
}
// modified end
void MDEngineMock::book_loop()
{
	KF_LOG_INFO(logger, "book_loop start");
	int64_t sleeping_time;
	int64_t book_data_pre = -1;
    int64_t book_data_now = -1; 
	//都有数据 且 trade比book早 book先等
	if(trade_data_firtm!=-1 && book_data_firtm!=-1 && trade_data_firtm < book_data_firtm) 
		this_thread::sleep_for(chrono::nanoseconds((book_data_firtm - trade_data_firtm)/playback_rate));
	while(isRunning)
	{
	    KF_LOG_INFO(logger, "[book_loop] (onDepth) " );
		if(!onDepth()) 
			break; // end of data
		book_data_now = book_line_data.begin()->first; //第二条的时间
		//第一条 计算等待时间
		if(book_data_pre == -1) 
			sleeping_time = (book_data_now - book_data_firtm) / playback_rate;
		//不是第一条 计算等待时间
		else 
			sleeping_time = (book_data_now - book_data_pre) / playback_rate;
		KF_LOG_INFO(logger, "delta time = " << (book_data_now - book_data_pre) << ",sleeping_time = "<< sleeping_time );
		book_data_pre = book_data_now;
		if(sleeping_time > 150000)
            this_thread::sleep_for(chrono::nanoseconds(sleeping_time)); // 每sleeping_time时间存一条数据
	}
}

void MDEngineMock::trade_loop()
{
	KF_LOG_INFO(logger, "trade_loop start");
	int64_t sleeping_time;
	int64_t trade_data_pre = -1;
    int64_t trade_data_now = -1;
	if(trade_data_firtm!=-1 && book_data_firtm!=-1 && book_data_firtm < trade_data_firtm) 
		this_thread::sleep_for(chrono::nanoseconds((trade_data_firtm - book_data_firtm)/playback_rate));
	while(isRunning)
	{
		KF_LOG_INFO(logger, "[trade_loop] (onFills) " );
		if(!onFills()) 
			break; // end of data
		trade_data_now = trade_line_data.begin()->first; 
        if(trade_data_pre == -1) 
			sleeping_time = (trade_data_now - trade_data_firtm) / playback_rate;
		else 
			sleeping_time = (trade_data_now - trade_data_pre) / playback_rate;
		trade_data_pre = trade_data_now;
		if(sleeping_time > 150000)
			this_thread::sleep_for(chrono::nanoseconds(sleeping_time));
	}
}

BOOST_PYTHON_MODULE(libmockmd)
{
    using namespace boost::python;
    class_<MDEngineMock, boost::shared_ptr<MDEngineMock> >("Engine")
    .def(init<>())
    .def("init", &MDEngineMock::initialize)
    .def("start", &MDEngineMock::start)
    .def("stop", &MDEngineMock::stop)
    .def("logout", &MDEngineMock::logout)
    .def("wait_for_stop", &MDEngineMock::wait_for_stop);
}
