import pandas as pd
import csv
import time
import datetime
"""
需要改变 kucoin_105.csv kucoin_106.csv mock_204.csv的文件抬头
kucoin_105
TradeTime,ExchangeID,InstrumentID,Price,Volume,OrderKind,OrderBSFlag,MakerOrderID,TakerOrderID,TradeID,Sequence,Status,h_extra_nano,h_nano,h_msg_type,h_request_id,h_source,h_is_last,h_error_id,j_name
kucoin_106
InstrumentID,ExchangeID,UpdateMicroSecond,BidLevelCount,AskLevelCount,BidLevels,AskLevels,Status,h_extra_nano,h_nano,h_msg_type,h_request_id,h_source,h_is_last,h_error_id,j_name
mock_204
BrokerID,UserID,InvestorID,BusinessUnit,ExchangeID,InstrumentID,OrderRef,LimitPrice,Volume,MinVolume,TimeCondition,VolumeCondition,OrderPriceType,Direction,OffsetFlag,HedgeFlag,ForceCloseReason,StopPrice,IsAutoSuspend,ContingentCondition,MiscInfo,MassOrderSeqId,MassOrderIndex,MassOrderTotalNum,ExpectPrice,h_extra_nano,h_nano,h_msg_type,h_request_id,h_source,h_is_last,h_error_id,j_name
mock_207
BrokerID,InvestorID,InstrumentID,ExchangeID,UserID,OrderRef,OrderSysID,RequestID,ActionFlag,LimitPrice,VolumeChange,KfOrderID,MiscInfo,MassOrderSeqId,MassOrderIndex,MassOrderTotalNum,h_extra_nano,h_nano,h_msg_type,h_request_id,h_source,h_is_last,h_error_id,j_name
"""

file_105 = pd.read_csv('kucoin_105.csv')
data_105 = pd.DataFrame(file_105) # LFL2TradeField
size_105 = len(data_105)
# print(data_105['h_nano'][1])
# print(size_105) 不包括表头
# print(data_105.iloc[1]) 从0开始
# print(data_105.iloc[0]['ExchangeID'])

file_106 = pd.read_csv('kucoin_106.csv')
data_106 = pd.DataFrame(file_106) # LFPriceBook20Field
size_106 = len(data_106)

file_204 = pd.read_csv('mock_204.csv')
data_204 = pd.DataFrame(file_204) # LFInputOrderField
size_204 = len(data_204)

file_207 = pd.read_csv('mock_207.csv')
data_207 = pd.DataFrame(file_207) 
size_207 = len(data_207)

file_td_207 = pd.read_csv('td_mock_207.csv')
data_td_207 = pd.DataFrame(file_td_207) 
size_td_207 = len(data_td_207)

mock_205 = open("mock_205.csv","w",newline='')
writer_205 =  csv.writer(mock_205)
writer_205.writerow(["BrokerID","UserID","ParticipantID","InvestorID","BusinessUnit","InstrumentID","OrderRef","ExchangeID","LimitPrice","VolumeTraded","VolumeTotal","VolumeTotalOriginal","TimeCondition","VolumeCondition","OrderPriceType","Direction","OffsetFlag","HedgeFlag","OrderStatus","RequestID","GatewayTime","h_nano"])
# rtn_order = {"BrokerID":"","UserID":"","ParticipantID":"","InvestorID":"","BusinessUnit":"","InstrumentID":"","OrderRef":"","ExchangeID":"","LimitPrice":0,"VolumeTraded":0,"VolumeTotal":0,"VolumeTotalOriginal":0,"TimeCondition":"","VolumeCondition":"","OrderPriceType":"","Direction":"","OffsetFlag":"","HedgeFlag":"","OrderStatus":"","RequestID":0,"GatewayTime":"","h_nano":"" }

mock_206 = open("mock_206.csv","w",newline='')
writer_206 = csv.writer(mock_206)
writer_206.writerow(["BrokerID","UserID","InvestorID","BusinessUnit","InstrumentID","OrderRef","ExchangeID","TradeID","OrderSysID","ParticipantID","ClientID","Price","Volume","TradingDay","TradeTime","Direction","OffsetFlag","HedgeFlag"])

mock_106 = open("mock_106.csv","w",newline='')
writer_106 = csv.writer(mock_106)
writer_106.writerow(["InstrumentID","ExchangeID","UpdateMicroSecond","BidLevelCount","AskLevelCount","BidLevels","AskLevels","Status"])

LOG = open("log.txt","w")

orderbook_buy_line = pd.DataFrame({"BrokerID":[],"UserID":[],"ParticipantID":[],"InvestorID":[],"BusinessUnit":[],"InstrumentID":[],"OrderRef":[],"ExchangeID":[],"LimitPrice":[],"VolumeTraded":[],"VolumeTotal":[],"VolumeTotalOriginal":[],"TimeCondition":[],"VolumeCondition":[],"OrderPriceType":[],"Direction":[],"OffsetFlag":[],"HedgeFlag":[],"OrderStatus":[],"RequestID":[],"GatewayTime":[],"h_nano":[]})
# print(orderbook_buy_line)
# orderbook_buy_line.sort_values(by=['LimitPrice','h_nano'],ascending=(False,True),inplace=True)
# orderbook_sell_line.sort_values(by=['LimitPrice','h_nano'],ascending=(True,True),inplace=True)
orderbook_sell_line = pd.DataFrame({"BrokerID":[],"UserID":[],"ParticipantID":[],"InvestorID":[],"BusinessUnit":[],"InstrumentID":[],"OrderRef":[],"ExchangeID":[],"LimitPrice":[],"VolumeTraded":[],"VolumeTotal":[],"VolumeTotalOriginal":[],"TimeCondition":[],"VolumeCondition":[],"OrderPriceType":[],"Direction":[],"OffsetFlag":[],"HedgeFlag":[],"OrderStatus":[],"RequestID":[],"GatewayTime":[],"h_nano":[]})
# std::map<int64_t,map<int64_t,LFRtnOrderField>,std::greater<int64_t> > orderbook_buy_line;
# std::map<int64_t,map<int64_t,LFRtnOrderField> > orderbook_sell_line;
not_canceled = []

def getTimestamp():
    t = time.time()
    return int(round(t * 1000000))*1000

def write_orderbook(InstrumentID, ExchangeID, h_nano, msg_type):
    orderbook ={"InstrumentID":"","ExchangeID":"","UpdateMicroSecond":"","BidLevelCount":0,"AskLevelCount":0,"BidLevels":{"volume":[],"price":[]},"AskLevels":{"volume":[],"price":[]},"Status":0 ,"msg_type:":""}
    orderbook["InstrumentID"] = InstrumentID
    orderbook["ExchangeID"] = ExchangeID
    orderbook["UpdateMicroSecond"] = h_nano

    orderbook["BidLevelCount"] = len(orderbook_buy_line.groupby("LimitPrice"))
    for name,group in orderbook_buy_line.groupby("LimitPrice"): # 按价格从大到小
        orderbook["BidLevels"]["price"].append(name)
        orderbook["BidLevels"]["volume"].append(group["VolumeTotalOriginal"].sum())  
    orderbook["BidLevels"]["price"].reverse()  
    orderbook["BidLevels"]["volume"].reverse()

    orderbook["AskLevelCount"] = len(orderbook_sell_line.groupby("LimitPrice"))
    for name,group in orderbook_sell_line.groupby("LimitPrice"): # 按价格从小到大
        orderbook["AskLevels"]["price"].append(name)
        orderbook["AskLevels"]["volume"].append(group["VolumeTotalOriginal"].sum())
    orderbook["msg_type"] = msg_type
    writer_106.writerow(list(orderbook.values()))
    
def updateOrderBook106(direction,price,volume):
    print("[updateOrderBook106] direction: {0}, price:{1}, volume{2}".format(direction,price,volume))
    LOG.write("\n"+"[updateOrderBook106] direction: {0}, price:{1}, volume{2}".format(direction,price,volume))
    orderbook_buy_line.sort_values(by=['LimitPrice','h_nano'],ascending=(False,True),inplace=True)
    orderbook_sell_line.sort_values(by=['LimitPrice','h_nano'],ascending=(True,True),inplace=True)
    buy_price = sell_price = -1    
    if not orderbook_buy_line.empty:
        buy_price = orderbook_buy_line.iloc[0]["LimitPrice"]
    if not orderbook_sell_line.empty:
        sell_price = orderbook_sell_line.iloc[0]["LimitPrice"]
    rtn_trade = {"BrokerID":"","UserID":"","InvestorID":"","BusinessUnit":"","InstrumentID":"","OrderRef":"","ExchangeID":"","TradeID":"","OrderSysID":"","ParticipantID":"","ClientID":"","Price":0,"Volume":0,"TradingDay":"","TradeTime":"","Direction":"","OffsetFlag":"","HedgeFlag":""}
    if (direction == 1 ):#sell 
        print("[updateOrderBook106] buy_price"+str(buy_price))
        LOG.write("\n"+"[updateOrderBook106] buy_price"+str(buy_price))
        if( price <= buy_price ):
            i = 0
            while( i < len(orderbook_buy_line)):
                if(volume <= 0):
                    break
                rtn_order = orderbook_buy_line.iloc[i].copy()
                buy_price = rtn_order["LimitPrice"]
                if(price > buy_price):
                    break
                if(rtn_order["RequestID"] == -1): # order from 106
                    if( volume<= rtn_order["VolumeTotalOriginal"]):
                        volume= 0
                    else:
                       volume-= rtn_order["VolumeTotalOriginal"]
                    i += 1
                    print("[updateOrderBook106] direction: from 106")
                    LOG.write("\n"+"[updateOrderBook106] direction: from 106")
                else: # order from local
                    volume_traded = min(volume,rtn_order["VolumeTotalOriginal"])
                    price_traded = rtn_order["LimitPrice"]
                    # 卖家
                    volume -= volume_traded
                    # 买家回报
                    rtn_order["VolumeTraded"] = volume_traded
                    rtn_order["VolumeTotal"] = rtn_order["VolumeTotalOriginal"] - volume_traded
                    if(rtn_order["VolumeTotal"] == 0):
                        rtn_order["OrderStatus"] = "AllTraded"
                    else:
                        rtn_order["OrderStatus"] = "LF_CHAR_PartTradedQueueing"
                    rtn_order["GatewayTime"] = getTimestamp()
                   # LOG.write("\n 106 1 order_ref"+str(rtn_order["OrderRef"] ))
                   # writer_205.writerow(rtn_order) 

                    rtn_trade["BrokerID"] = rtn_order["BrokerID"]   
                    rtn_trade["UserID"] = rtn_order["UserID"]
                    rtn_trade["InvestorID"] = rtn_order["InvestorID"]
                    rtn_trade["BusinessUnit"] = rtn_order["BusinessUnit"]
                    rtn_trade["InstrumentID"] = rtn_order["InstrumentID"]
                    rtn_trade["OrderRef"] = rtn_order["OrderRef"]
                    rtn_trade["ExchangeID"] = "MOCK"
                    rtn_trade["TradeID"] = getTimestamp()
                    rtn_trade["OrderSysID"] = rtn_order["OrderRef"]
                    rtn_trade["Direction"] = rtn_order["Direction"]
                    rtn_trade["OffsetFlag"] = rtn_order["OffsetFlag"]
                    rtn_trade["HedgeFlag"] = rtn_order["HedgeFlag"]
                    rtn_trade["Price"] = price_traded
                    rtn_trade["Volume"] = volume_traded
                    rtn_trade["TradeTime"] = getTimestamp() 
                    #writer_206.writerow(list(rtn_trade.values()))
                    if(rtn_order["VolumeTotal"] == 0):
                        orderbook_buy_line.drop(orderbook_buy_line.index[i],inplace=True)
                    else:
                        orderbook_buy_line.iloc[i,9] = 0 # "VolumeTraded"
                        orderbook_buy_line.iloc[i,11] = rtn_order["VolumeTotal"] # "VolumeTotalOriginal"
                        i += 1
        if direction ==0:#buy
            print("[updateOrderBook106] sell_price"+str(sell_price) )
            if( price >= sell_price ):
                i = 0
                while( i < len(orderbook_sell_line) ):
                    if(volume<= 0):
                        break
                    rtn_order = orderbook_sell_line.iloc[i].copy()
                    sell_price = rtn_order["LimitPrice"]
                    if(price < sell_price):
                        break
                    if(rtn_order["RequestID"] == -1): # order from 106
                        if( volume <= rtn_order["VolumeTotalOriginal"]):
                            volume= 0
                        else:
                            volume-= rtn_order["VolumeTotalOriginal"]
                        i += 1
                        print("[updateOrderBook106] direction: from 106")
                        LOG.write("\n"+"[updateOrderBook106] direction: from 106")
                    else: # order from local 
                        print("[updateOrderBook106] direction: from LOCAL!!!!!")
                        LOG.write("\n"+"[updateOrderBook106] direction: from  LOCAL!!!!!")
                        volume_traded = min(volume,rtn_order["VolumeTotalOriginal"])
                        price_traded = min(price,rtn_order["LimitPrice"])
                        # 买家
                        volume-= volume_traded
                        # 卖家回报
                        rtn_order["VolumeTraded"] = volume_traded
                        rtn_order["VolumeTotal"] = rtn_order["VolumeTotalOriginal"] - volume_traded
                        if(rtn_order["VolumeTotal"] == 0):
                            rtn_order["OrderStatus"] = "AllTraded"
                        else:
                            rtn_order["OrderStatus"] = "PartTradedQueueing"
                        rtn_order["GatewayTime"] = getTimestamp()
                        LOG.write("\n 106 2 order_ref"+str(rtn_order["OrderRef"] ))
                       # writer_205.writerow(rtn_order) 

                        rtn_trade["BrokerID"] = rtn_order["BrokerID"]   
                        rtn_trade["UserID"] = rtn_order["UserID"]
                        rtn_trade["InvestorID"] = rtn_order["InvestorID"]
                        rtn_trade["BusinessUnit"] = rtn_order["BusinessUnit"]
                        rtn_trade["InstrumentID"] = rtn_order["InstrumentID"]
                        rtn_trade["OrderRef"] = rtn_order["OrderRef"]
                        rtn_trade["ExchangeID"] = "MOCK"
                        rtn_trade["TradeID"] = getTimestamp()
                        rtn_trade["OrderSysID"] = rtn_order["OrderRef"]
                        rtn_trade["Direction"] = rtn_order["Direction"]
                        rtn_trade["OffsetFlag"] = rtn_order["OffsetFlag"]
                        rtn_trade["HedgeFlag"] = rtn_order["HedgeFlag"]
                        rtn_trade["Price"] = price_traded
                        rtn_trade["Volume"] = volume_traded
                        rtn_trade["TradeTime"] = getTimestamp() 
                        #writer_206.writerow(list(rtn_trade.values()))

                        if(rtn_order["VolumeTotal"] == 0):
                            orderbook_sell_line.drop(orderbook_sell_line.index[i],inplace=True)
                        else:
                            orderbook_sell_line.iloc[i,9] = 0 # "VolumeTraded"
                            orderbook_sell_line.iloc[i,11] = rtn_order["VolumeTotal"] # VolumeTotalOriginal
                            i += 1  

             

if __name__ == "__main__":
    iter_105 = 0
    iter_106 = 0
    iter_204 = 0
    iter_207 = 0
    count = 0
    iter_td_207 = 0
    while iter_td_207<size_td_207 :
        errorbook = data_td_207.iloc[iter_td_207].copy()
        not_canceled.append(errorbook["OrderRef(c21)"])
        iter_td_207 += 1
    # print(len(not_canceled))
    while iter_105<size_105 or iter_106<size_106 or iter_204<size_204 or iter_207<size_207:
        print(count)
        if iter_105<size_105 :
            time_105 = data_105['h_nano'][iter_105]
        else:
            time_105 = 9999999999999999999
        if iter_106<size_106 :
            time_106 = data_106['h_nano'][iter_106]
        else:
            time_106 = 9999999999999999999
        if iter_204<size_204 :
            time_204 = data_204['h_nano'][iter_204]
        else:
            time_204 = 9999999999999999999
        if iter_207<size_207 :
            time_207 = data_207['h_nano'][iter_207]
        else:
            time_207 = 9999999999999999999
        count += 1
        if time_105 <= time_106 and time_105 <= time_204 and time_105 <= time_207: # receive msg 105
            print("105")
            orderbook_buy_line.sort_values(by=['LimitPrice','h_nano'],ascending=(False,True),inplace=True)
            orderbook_sell_line.sort_values(by=['LimitPrice','h_nano'],ascending=(True,True),inplace=True)
            one_order = data_105.iloc[iter_105].copy()
            iter_105 += 1
            buy_price = sell_price = -1    
            if not orderbook_buy_line.empty:
                buy_price = orderbook_buy_line.iloc[0]["LimitPrice"]
            if not orderbook_sell_line.empty:
                sell_price = orderbook_sell_line.iloc[0]["LimitPrice"]
            rtn_trade = {"BrokerID":"","UserID":"","InvestorID":"","BusinessUnit":"","InstrumentID":"","OrderRef":"","ExchangeID":"","TradeID":"","OrderSysID":"","ParticipantID":"","ClientID":"","Price":0,"Volume":0,"TradingDay":"","TradeTime":"","Direction":"","OffsetFlag":"","HedgeFlag":""}
            if( one_order["Price"] <= buy_price ):
                # orderbook_buy_line
                i = 0
                while( i < len(orderbook_buy_line)):
                    if(one_order["Volume"] <= 0):
                        break
                    rtn_order = orderbook_buy_line.iloc[i].copy()
                    buy_price = rtn_order["LimitPrice"]
                    if(one_order["Price"] > buy_price):
                        break
                    if(rtn_order["RequestID"] == -1): # order from 106
                        if( one_order["Volume"] <= rtn_order["VolumeTotalOriginal"]):
                            one_order["Volume"] = 0
                        else:
                            one_order["Volume"] -= rtn_order["VolumeTotalOriginal"]
                        i += 1
                    else: # order from local
                        volume_traded = min(one_order["Volume"],rtn_order["VolumeTotalOriginal"])
                        price_traded = rtn_order["LimitPrice"]
                        # 卖家
                        one_order["Volume"] -= volume_traded
                        # 买家回报
                        rtn_order["VolumeTraded"] = volume_traded
                        rtn_order["VolumeTotal"] = rtn_order["VolumeTotalOriginal"] - volume_traded
                        if(rtn_order["VolumeTotal"] == 0):
                            rtn_order["OrderStatus"] = "AllTraded"
                        else:
                            rtn_order["OrderStatus"] = "LF_CHAR_PartTradedQueueing"
                        rtn_order["GatewayTime"] = getTimestamp()
                        LOG.write("\n 105 order_ref"+str(rtn_order["OrderRef"] ))
                        writer_205.writerow(rtn_order) 

                        rtn_trade["BrokerID"] = rtn_order["BrokerID"]   
                        rtn_trade["UserID"] = rtn_order["UserID"]
                        rtn_trade["InvestorID"] = rtn_order["InvestorID"]
                        rtn_trade["BusinessUnit"] = rtn_order["BusinessUnit"]
                        rtn_trade["InstrumentID"] = rtn_order["InstrumentID"]
                        rtn_trade["OrderRef"] = rtn_order["OrderRef"]
                        rtn_trade["ExchangeID"] = "MOCK"
                        rtn_trade["TradeID"] = getTimestamp()
                        rtn_trade["OrderSysID"] = rtn_order["OrderRef"]
                        rtn_trade["Direction"] = rtn_order["Direction"]
                        rtn_trade["OffsetFlag"] = rtn_order["OffsetFlag"]
                        rtn_trade["HedgeFlag"] = rtn_order["HedgeFlag"]
                        rtn_trade["Price"] = price_traded
                        rtn_trade["Volume"] = volume_traded
                        rtn_trade["TradeTime"] = getTimestamp() 
                        writer_206.writerow(list(rtn_trade.values()))

                        if(rtn_order["VolumeTotal"] == 0):
                            orderbook_buy_line.drop(orderbook_buy_line.index[i],inplace=True)
                        else:
                            orderbook_buy_line.iloc[i,9] = 0 # "VolumeTraded"
                            orderbook_buy_line.iloc[i,11] = rtn_order["VolumeTotal"] # "VolumeTotalOriginal"
                            i += 1
            elif( sell_price != -1 and one_order["Price"] >= sell_price):
                # orderbook_sell_line
                i = 0
                while( i < len(orderbook_sell_line) ):
                    if(one_order["Volume"] <= 0):
                        break
                    rtn_order = orderbook_sell_line.iloc[i].copy()
                    sell_price = rtn_order["LimitPrice"]
                    if(one_order["Price"] < sell_price):
                        break
                    if(rtn_order["RequestID"] == -1): # order from 106
                        if( one_order["Volume"] <= rtn_order["VolumeTotalOriginal"]):
                            one_order["Volume"] = 0
                        else:
                            one_order["Volume"] -= rtn_order["VolumeTotalOriginal"]
                        i += 1
                    else: # order from local 
                        volume_traded = min(one_order["Volume"],rtn_order["VolumeTotalOriginal"])
                        price_traded = min(one_order["Price"],rtn_order["LimitPrice"])
                        # 买家
                        one_order["Volume"] -= volume_traded
                        # 卖家回报
                        rtn_order["VolumeTraded"] = volume_traded
                        rtn_order["VolumeTotal"] = rtn_order["VolumeTotalOriginal"] - volume_traded
                        if(rtn_order["VolumeTotal"] == 0):
                            rtn_order["OrderStatus"] = "AllTraded"
                        else:
                            rtn_order["OrderStatus"] = "PartTradedQueueing"
                        rtn_order["GatewayTime"] = getTimestamp()
                        LOG.write("\n 105 2 order_ref"+str(rtn_order["OrderRef"] ))
                        writer_205.writerow(rtn_order) 

                        rtn_trade["BrokerID"] = rtn_order["BrokerID"]   
                        rtn_trade["UserID"] = rtn_order["UserID"]
                        rtn_trade["InvestorID"] = rtn_order["InvestorID"]
                        rtn_trade["BusinessUnit"] = rtn_order["BusinessUnit"]
                        rtn_trade["InstrumentID"] = rtn_order["InstrumentID"]
                        rtn_trade["OrderRef"] = rtn_order["OrderRef"]
                        rtn_trade["ExchangeID"] = "MOCK"
                        rtn_trade["TradeID"] = getTimestamp()
                        rtn_trade["OrderSysID"] = rtn_order["OrderRef"]
                        rtn_trade["Direction"] = rtn_order["Direction"]
                        rtn_trade["OffsetFlag"] = rtn_order["OffsetFlag"]
                        rtn_trade["HedgeFlag"] = rtn_order["HedgeFlag"]
                        rtn_trade["Price"] = price_traded
                        rtn_trade["Volume"] = volume_traded
                        rtn_trade["TradeTime"] = getTimestamp() 
                        writer_206.writerow(list(rtn_trade.values()))

                        if(rtn_order["VolumeTotal"] == 0):
                            orderbook_sell_line.drop(orderbook_sell_line.index[i],inplace=True)
                        else:
                            orderbook_sell_line.iloc[i,9] = 0 # "VolumeTraded"
                            orderbook_sell_line.iloc[i,11] = rtn_order["VolumeTotal"] # VolumeTotalOriginal
                            i += 1                                       
            write_orderbook(one_order["InstrumentID"],one_order["ExchangeID"],one_order["h_nano"],"105")
        elif time_106 <= time_105 and time_106 <= time_204 and time_106 <= time_207: # receive msg 106 , update orderbook
            print("106")
            orderbook = data_106.iloc[iter_106].copy()
            iter_106 += 1
            list1 = orderbook["BidLevels"].split(";")
            orderbook["BidLevels"] = {"volume":[],"price":[]}
            for volume_price in list1:
                if(volume_price == ""): continue
                list2 = volume_price.split("@")
                orderbook["BidLevels"]["volume"].append(int(list2[0]))
                orderbook["BidLevels"]["price"].append(int(list2[1]))
            list1 = orderbook["AskLevels"].split(";")
            orderbook["AskLevels"] = {"volume":[],"price":[]}
            for volume_price in list1:
                if(volume_price == ""): continue
                list2 = volume_price.split("@")
                orderbook["AskLevels"]["volume"].append(int(list2[0]))
                orderbook["AskLevels"]["price"].append(int(list2[1]))
            # print(orderbook["AskLevels"]["price"])
            orderbook_buy_line.sort_values(by=['LimitPrice','h_nano'],ascending=(False,True),inplace=True)
            orderbook_sell_line.sort_values(by=['LimitPrice','h_nano'],ascending=(True,True),inplace=True)  
            rtn_order = {"BrokerID":"","UserID":"","ParticipantID":"","InvestorID":"","BusinessUnit":"","InstrumentID":orderbook["InstrumentID"],"OrderRef":"","ExchangeID":orderbook["ExchangeID"],"LimitPrice":0,"VolumeTraded":0,"VolumeTotal":0,"VolumeTotalOriginal":0,"TimeCondition":"","VolumeCondition":"","OrderPriceType":"","Direction":"","OffsetFlag":"","HedgeFlag":"","OrderStatus":"","RequestID":0,"GatewayTime":"","h_nano":orderbook["h_nano"] }
            rtn_orders = []
            # check orderbook_buy_line , 双向搜索
            i = j = k = 0
            while( i < len(orderbook_buy_line) or j < orderbook["BidLevelCount"]):
                # [i,k)的价格为一个level  i直接跳转 = k
                if(i < len(orderbook_buy_line)):
                    local_price = orderbook_buy_line.iloc[i]["LimitPrice"]
                    local_volume = 0
                    k = i 
                    while( k < len(orderbook_buy_line)):
                        if(orderbook_buy_line.iloc[k]["LimitPrice"] == local_price):
                            if(orderbook_buy_line.iloc[k]["RequestID"] == -1):
                                local_volume += orderbook_buy_line.iloc[k]["VolumeTotalOriginal"] 
                            k += 1
                        else:
                            break
                    # online一个level
                else:
                    local_price = -1
                    local_volume = 0
                if(j < orderbook["BidLevelCount"]):
                    online_price = orderbook["BidLevels"]["price"][j]
                    online_volume = orderbook["BidLevels"]["volume"][j]
                else:
                    online_price = -1
                # print("in circle","i=",i,",j=",j,",k=",k)
                # print("local_price=",local_price,",local_volume=",local_volume)
                # print("online_price=",online_price,",online_volume=",online_volume)
                if(local_price < online_price):
                    # move to local orderbook
                    updateOrderBook106(1,online_price,online_volume)
                    if online_volume <=0:
                        continue
                    rtn_order["LimitPrice"] = online_price
                    rtn_order["RequestID"] = -1
                    rtn_order["OrderRef"] = -1
                    rtn_order["VolumeTotalOriginal"] = online_volume
                    rtn_order["VolumeTraded"] = rtn_order["VolumeTotal"] = 0
                    rtn_orders.append(rtn_order.copy())
                    j += 1
                elif(local_price > online_price):
                    # remove requestID == -1
                    # print("local_price > online_price |", local_price,">",online_price)
                    while(i < k):
                        if(orderbook_buy_line.iloc[i]["RequestID"]==-1):
                            orderbook_buy_line.drop(orderbook_buy_line.index[i],inplace=True)
                            k -= 1
                        else:
                            i += 1
                    i = k
                else: # local_price == online_price
                    if(local_volume < online_volume):
                        delta_volume = online_volume - local_volume
                        if(orderbook_buy_line.iloc[k-1]["RequestID"] == -1): # 直接追加
                            new_volume = orderbook_buy_line.iloc[k-1]["VolumeTotalOriginal"] + delta_volume
                            orderbook_buy_line.iloc[k-1,11] = new_volume # 11 VolumeTotalOriginal
                        else: # 新建order
                            rtn_order["LimitPrice"] = online_price
                            rtn_order["RequestID"] = -1
                            rtn_order["OrderRef"] = -1
                            rtn_order["VolumeTotalOriginal"] = delta_volume
                            rtn_order["VolumeTraded"] = rtn_order["VolumeTotal"] = 0
                            rtn_orders.append(rtn_order)
                    elif(local_volume > online_volume):
                        delta_volume = local_volume - online_volume
                        while(i < k):
                            if(delta_volume == 0):
                                break
                            if(orderbook_buy_line.iloc[i]["RequestID"] == -1):
                                if(orderbook_buy_line.iloc[i]["VolumeTotalOriginal"] > delta_volume):
                                    # print(orderbook_buy_line.iloc[i]["VolumeTotalOriginal"])
                                    # print(delta_volume)
                                    new_volume = orderbook_buy_line.iloc[i]["VolumeTotalOriginal"] - delta_volume
                                    orderbook_buy_line.iloc[i,11] = new_volume
                                    # print(orderbook_buy_line.iloc[i]["VolumeTotalOriginal"])
                                    delta_volume = 0
                                    i += 1
                                else:
                                    delta_volume -= orderbook_buy_line.iloc[i]["VolumeTotalOriginal"]
                                    orderbook_buy_line.drop(orderbook_buy_line.index[i],inplace=True)
                                    k -= 1
                            else:
                                i += 1
                    i = k
                    j += 1
            # add rtn_orders
            for rtn_order_a in rtn_orders:
                # print(rtn_order_a["LimitPrice"])
                orderbook_buy_line = orderbook_buy_line.append(rtn_order_a,ignore_index=True)
            # debug print(orderbook_buy_line["VolumeTotalOriginal"])
            rtn_orders = []
            # check orderbook_sell_line
            i = j = k = 0
            while( i < len(orderbook_sell_line) or j < orderbook["AskLevelCount"]):
                # [i,k)的价格为一个level  i直接跳转 = k
                if(i < len(orderbook_sell_line)):
                    local_price = orderbook_sell_line.iloc[i]["LimitPrice"]
                    local_volume = 0
                    k = i 
                    while( k < len(orderbook_sell_line)):
                        if(orderbook_sell_line.iloc[k]["LimitPrice"] == local_price):
                            if(orderbook_sell_line.iloc[k]["RequestID"] == -1):
                                local_volume += orderbook_sell_line.iloc[k]["VolumeTotalOriginal"] 
                            k += 1
                        else:
                            break
                else:
                    local_price = 9999999999999999999
                # print("local_price=",local_price,",local_volume=",local_volume)
                # print("online_price=",online_price,",online_volume=",online_volume)
                if(j < orderbook["AskLevelCount"]):
                    online_price = orderbook["AskLevels"]["price"][j]
                    online_volume = orderbook["AskLevels"]["volume"][j]
                else:
                    online_price = 9999999999999999999
                if(local_price > online_price):
                    # move to local orderbook
                    updateOrderBook106(0,online_price,online_volume)
                    if online_volume <= 0 :
                        continue
                    rtn_order["LimitPrice"] = online_price
                    rtn_order["RequestID"] = -1
                    rtn_order["VolumeTotalOriginal"] = online_volume
                    rtn_order["VolumeTraded"] = rtn_order["VolumeTotal"] = 0
                    rtn_orders.append(rtn_order.copy())
                    j += 1
                elif(local_price < online_price):
                    # remove requestID == -1
                    while(i < k):
                        if(orderbook_sell_line.iloc[i]["RequestID"]==-1):
                            orderbook_sell_line.drop(orderbook_sell_line.index[i],inplace=True)
                            k -= 1
                        else:
                            i += 1
                    i = k
                else: # local_price == online_price
                    if(local_volume < online_volume):
                        delta_volume = online_volume - local_volume
                        if(orderbook_sell_line.iloc[k-1]["RequestID"] == -1): # 直接追加
                            new_volume = orderbook_sell_line.iloc[k-1]["VolumeTotalOriginal"] + delta_volume
                            orderbook_sell_line.iloc[k-1,11] = new_volume
                        else: # 新建order
                            rtn_order["LimitPrice"] = online_price
                            rtn_order["RequestID"] = -1
                            rtn_order["VolumeTotalOriginal"] = delta_volume
                            rtn_order["VolumeTraded"] = rtn_order["VolumeTotal"] = 0
                            rtn_orders.append(rtn_order.copy())
                    elif(local_volume > online_volume):
                        delta_volume = local_volume - online_volume
                        while(i < k):
                            if(delta_volume == 0):
                                break
                            if(orderbook_sell_line.iloc[i]["RequestID"] == -1):
                                if(orderbook_sell_line.iloc[i]["VolumeTotalOriginal"] > delta_volume):
                                    new_volume = orderbook_sell_line.iloc[i]["VolumeTotalOriginal"] - delta_volume
                                    orderbook_sell_line.iloc[i,11] = new_volume
                                    delta_volume = 0
                                    i += 1
                                else:
                                    delta_volume -= orderbook_sell_line.iloc[i]["VolumeTotalOriginal"]
                                    orderbook_sell_line.drop(orderbook_sell_line.index[i],inplace=True)
                                    k -= 1
                            else:
                                i += 1
                    i = k
                    j += 1
            # add rtn_orders
            for rtn_order_a in rtn_orders:
                orderbook_sell_line = orderbook_sell_line.append(rtn_order_a,ignore_index=True) 
            # debug print(orderbook_sell_line)     
            write_orderbook(orderbook["InstrumentID"],orderbook["ExchangeID"],orderbook["h_nano"],"106")      
        elif time_204 <= time_105 and time_204 <= time_106 and time_204 <= time_207: # receive msg 204
            print("204")
            LOG.write("\n"+"204 ")
            # insert an order: data_204[]
            one_order = data_204.iloc[iter_204].copy()
            iter_204 += 1
            LOG.write("\n"+str(one_order["OrderRef"]))
            LOG.write("\n"+'\n')
            data_price = one_order["LimitPrice"]
            data_volume = one_order["Volume"]
            LOG.write("\n"+" data_volume=")
            LOG.write("\n"+str(data_volume))
            if(one_order["OrderPriceType"] == "AnyPrice"):
                if(one_order["Direction"] == "Sell"):
                    data_price = 0
                else:
                    data_price = 9223372036854775807
            rtn_order_A = {"BrokerID":"","UserID":"","ParticipantID":"","InvestorID":"","BusinessUnit":"","InstrumentID":"","OrderRef":"","ExchangeID":"","LimitPrice":0,"VolumeTraded":0,"VolumeTotal":0,"VolumeTotalOriginal":0,"TimeCondition":"","VolumeCondition":"","OrderPriceType":"","Direction":"","OffsetFlag":"","HedgeFlag":"","OrderStatus":"","RequestID":0,"GatewayTime":"","h_nano":"" }
            rtn_order_A["BrokerID"] = one_order["BrokerID"]
            rtn_order_A["UserID"] = one_order["UserID"]
            rtn_order_A["InvestorID"] = one_order["InvestorID"]
            rtn_order_A["BusinessUnit"] = one_order["BusinessUnit"]
            rtn_order_A["InstrumentID"] = one_order["InstrumentID"]
            rtn_order_A["OrderRef"] = one_order["OrderRef"]
            rtn_order_A["ExchangeID"] = "MOCK"
            rtn_order_A["LimitPrice"] = data_price
            rtn_order_A["VolumeTraded"] = 0
            rtn_order_A["VolumeTotal"] = rtn_order_A["VolumeTotalOriginal"] = one_order["Volume"]
            rtn_order_A["TimeCondition"] = one_order["TimeCondition"]
            rtn_order_A["VolumeCondition"] = one_order["VolumeCondition"]
            rtn_order_A["OrderPriceType"] = one_order["OrderPriceType"]
            rtn_order_A["Direction"] = one_order["Direction"]
            rtn_order_A["OffsetFlag"] = one_order["OffsetFlag"]
            rtn_order_A["HedgeFlag"] = one_order["HedgeFlag"]
            rtn_order_A["OrderStatus"] = "NotTouched"
            rtn_order_A["RequestID"] = 1
            rtn_order_A["h_nano"] = one_order["h_nano"]
            # print(rtn_order_A)
            LOG.write("\n 204 1 order_ref"+str(rtn_order_A["OrderRef"] ))
            writer_205.writerow(list(rtn_order_A.values()))
    
            rtn_trade_A = {"BrokerID":"","UserID":"","InvestorID":"","BusinessUnit":"","InstrumentID":"","OrderRef":"","ExchangeID":"","TradeID":"","OrderSysID":"","ParticipantID":"","ClientID":"","Price":0,"Volume":0,"TradingDay":"","TradeTime":"","Direction":"","OffsetFlag":"","HedgeFlag":""}
            rtn_trade_A["BrokerID"] = one_order["BrokerID"]
            rtn_trade_A["UserID"] = one_order["UserID"]
            rtn_trade_A["InvestorID"] = one_order["InvestorID"]
            rtn_trade_A["BusinessUnit"] = one_order["BusinessUnit"]
            rtn_trade_A["InstrumentID"] = one_order["InstrumentID"]
            rtn_trade_A["OrderRef"] = one_order["OrderRef"]
            rtn_trade_A["ExchangeID"] = "MOCK"
            rtn_trade_A["TradeID"] = getTimestamp()
            rtn_trade_A["OrderSysID"] = one_order["OrderRef"]
            rtn_trade_A["Direction"] = one_order["Direction"]
            rtn_trade_A["OffsetFlag"] = one_order["OffsetFlag"]
            rtn_trade_A["HedgeFlag"] = one_order["HedgeFlag"]    
            rtn_trade_B = {"BrokerID":"","UserID":"","InvestorID":"","BusinessUnit":"","InstrumentID":"","OrderRef":"","ExchangeID":"","TradeID":"","OrderSysID":"","ParticipantID":"","ClientID":"","Price":0,"Volume":0,"TradingDay":"","TradeTime":"","Direction":"","OffsetFlag":"","HedgeFlag":""}

            if(one_order["Direction"] == "Buy"):
                # print("Buy")
                # 查找orderbook_sell_line
                while( data_volume > 0):
                    LOG.write("\n"+" data_volume=")
                    LOG.write("\n"+str(data_volume))
                    if orderbook_sell_line.empty:
                        break
                    orderbook_sell_line.sort_values(by=['LimitPrice','h_nano'],ascending=(True,True),inplace=True)
                    # 获取第一行
                    rtn_order_B = orderbook_sell_line.iloc[0].copy()
                    sell_volume = rtn_order_B["VolumeTotalOriginal"]
                    sell_price = rtn_order_B["LimitPrice"]
                    if(sell_price > data_price):
                        break
                    orderbook_sell_line.drop(orderbook_sell_line.index[0],inplace=True)
                    volume_traded = min(sell_volume , data_volume)
                    price_traded = min(sell_price , data_price) # sell_price
                    # 买家回报
                    # 205
                    rtn_order_A["LimitPrice"] = price_traded
                    rtn_order_A["VolumeTraded"] = volume_traded
                    rtn_order_A["VolumeTotal"] = data_volume - volume_traded
                    rtn_order_A["VolumeTotalOriginal"] = data_volume
                    if(data_volume == volume_traded):
                        rtn_order_A["OrderStatus"] = "AllTraded"
                    else:
                        rtn_order_A["OrderStatus"] = "PartTradedQueueing"  
                    LOG.write("\n 204 2 order_ref"+str(rtn_order_A["OrderRef"] )) 
                    writer_205.writerow(list(rtn_order_A.values()))
                    # 206
                    rtn_trade_A["Price"] = price_traded
                    rtn_trade_A["Volume"] = volume_traded
                    rtn_trade_A["TradeTime"] = getTimestamp()
                    writer_206.writerow(list(rtn_trade_A.values()))
                    # 修改 volume
                    data_volume -= volume_traded
                    # 卖家回报
                    rtn_order_B["LimitPrice"] = price_traded
                    rtn_order_B["VolumeTraded"] = volume_traded
                    rtn_order_B["VolumeTotal"] = sell_volume - volume_traded
                    if(rtn_order_B["VolumeTotal"] == 0):
                        rtn_order_B["OrderStatus"] = "AllTraded"
                    else:
                        rtn_order_B["OrderStatus"] = "PartTradedQueueing"  
                    # 205
                    if(rtn_order_B["RequestID"] != -1):
                        writer_205.writerow(list(rtn_order_B))
                        LOG.write("\n 204 3 order_ref"+str(rtn_order_B["OrderRef"] ))
                    # 206
                        rtn_trade_B["BrokerID"] = rtn_order_B["BrokerID"]
                        rtn_trade_B["UserID"] = rtn_order_B["UserID"]
                        rtn_trade_B["InvestorID"] = rtn_order_B["InvestorID"]
                        rtn_trade_B["BusinessUnit"] = rtn_order_B["BusinessUnit"]
                        rtn_trade_B["InstrumentID"] = rtn_order_B["InstrumentID"]
                        rtn_trade_B["OrderRef"] = rtn_order_B["OrderRef"]
                        rtn_trade_B["ExchangeID"] = "MOCK"
                        rtn_trade_B["TradeID"] = getTimestamp()
                        rtn_trade_B["OrderSysID"] = rtn_order_B["OrderRef"]
                        rtn_trade_B["Direction"] = rtn_order_B["Direction"]
                        rtn_trade_B["OffsetFlag"] = rtn_order_B["OffsetFlag"]
                        rtn_trade_B["HedgeFlag"] = rtn_order_B["HedgeFlag"]
                        rtn_trade_B["Price"] = price_traded
                        rtn_trade_B["Volume"] = volume_traded
                        rtn_trade_B["TradeTime"] = getTimestamp()
                        writer_206.writerow(list(rtn_trade_B.values()))
                    if( rtn_order_B["VolumeTotal"] == 0 ):
                        continue
                    rtn_order_B["LimitPrice"] = sell_price
                    rtn_order_B["VolumeTraded"] = 0
                    rtn_order_B["VolumeTotalOriginal"] = rtn_order_B["VolumeTotal"]
                    orderbook_sell_line = orderbook_sell_line.append(rtn_order_B,ignore_index=True)
                if( data_volume > 0 ):
                    rtn_order_A["LimitPrice"] = data_price
                    rtn_order_A["VolumeTraded"] = 0
                    rtn_order_A["VolumeTotalOriginal"] = rtn_order_A["VolumeTotal"] = data_volume
                    orderbook_buy_line = orderbook_buy_line.append(rtn_order_A,ignore_index=True)
            elif(one_order["Direction"] == "Sell"):
                # print("Sell")
                # 查找orderbook_buy_line
                while( data_volume > 0 ):
                    if orderbook_buy_line.empty:
                        break
                    orderbook_buy_line.sort_values(by=['LimitPrice','h_nano'],ascending=(False,True),inplace=True)
                    # 获取第一行
                    rtn_order_B = orderbook_buy_line.iloc[0].copy()
                    buy_volume = rtn_order_B["VolumeTotalOriginal"]
                    buy_price = rtn_order_B["LimitPrice"]
                    if(buy_price < data_price):
                        break
                    orderbook_buy_line.drop(orderbook_buy_line.index[0],inplace=True)
                    volume_traded = min(buy_volume , data_volume)
                    price_traded = buy_price # data_price traded as the former
                    if(data_price == 0): price_traded = buy_price
                    # 卖家回报
                    # 205
                    rtn_order_A["LimitPrice"] = price_traded
                    rtn_order_A["VolumeTraded"] = volume_traded
                    rtn_order_A["VolumeTotal"] = data_volume - volume_traded
                    rtn_order_A["VolumeTotalOriginal"] = data_volume
                    if(data_volume == volume_traded):
                        rtn_order_A["OrderStatus"] = "AllTraded"
                    else:
                        rtn_order_A["OrderStatus"] = "PartTradedQueueing"   
                    writer_205.writerow(list(rtn_order_A.values()))
                    LOG.write("\n 204 4 order_ref"+str(rtn_order_A["OrderRef"] ))
                    # 206
                    rtn_trade_A["Price"] = price_traded
                    rtn_trade_A["Volume"] = volume_traded
                    rtn_trade_A["TradeTime"] = getTimestamp()
                    writer_206.writerow(list(rtn_trade_A.values()))
                    # 修改 volume
                    data_volume -= volume_traded
                    # 买家回报
                    rtn_order_B["LimitPrice"] = price_traded
                    rtn_order_B["VolumeTraded"] = volume_traded
                    rtn_order_B["VolumeTotal"] = buy_volume - volume_traded
                    if(rtn_order_B["VolumeTotal"] == 0):
                        rtn_order_B["OrderStatus"] = "AllTraded"
                    else:
                        rtn_order_B["OrderStatus"] = "PartTradedQueueing"  
                    # 205
                    if(rtn_order_B["RequestID"] != -1):
                        writer_205.writerow(list(rtn_order_B))
                        LOG.write("\n 204 5 order_ref"+str(rtn_order_B["OrderRef"] ))
                    # 206
                        rtn_trade_B["BrokerID"] = rtn_order_B["BrokerID"]
                        rtn_trade_B["UserID"] = rtn_order_B["UserID"]
                        rtn_trade_B["InvestorID"] = rtn_order_B["InvestorID"]
                        rtn_trade_B["BusinessUnit"] = rtn_order_B["BusinessUnit"]
                        rtn_trade_B["InstrumentID"] = rtn_order_B["InstrumentID"]
                        rtn_trade_B["OrderRef"] = rtn_order_B["OrderRef"]
                        rtn_trade_B["ExchangeID"] = "MOCK"
                        rtn_trade_B["TradeID"] = getTimestamp()
                        rtn_trade_B["OrderSysID"] = rtn_order_B["OrderRef"]
                        rtn_trade_B["Direction"] = rtn_order_B["Direction"]
                        rtn_trade_B["OffsetFlag"] = rtn_order_B["OffsetFlag"]
                        rtn_trade_B["HedgeFlag"] = rtn_order_B["HedgeFlag"]
                        rtn_trade_B["Price"] = price_traded
                        rtn_trade_B["Volume"] = volume_traded
                        rtn_trade_B["TradeTime"] = getTimestamp()
                        writer_206.writerow(list(rtn_trade_B.values()))
                    if( rtn_order_B["VolumeTotal"] == 0 ):
                        continue
                    rtn_order_B["LimitPrice"] = buy_price
                    rtn_order_B["VolumeTraded"] = 0
                    rtn_order_B["VolumeTotalOriginal"] = rtn_order_B["VolumeTotal"]
                    orderbook_buy_line = orderbook_buy_line.append(rtn_order_B,ignore_index=True)
                if( data_volume > 0 ):
                    rtn_order_A["LimitPrice"] = data_price
                    rtn_order_A["VolumeTraded"] = 0
                    rtn_order_A["VolumeTotalOriginal"] = rtn_order_A["VolumeTotal"] = data_volume
                    orderbook_sell_line = orderbook_sell_line.append(rtn_order_A,ignore_index=True)
            # print("orderbook_buy_line:")
            # print(orderbook_buy_line["OrderRef"])
            # print("orderbook_sell_line:")
            # print(orderbook_sell_line["OrderRef"])            
            write_orderbook(one_order["InstrumentID"],one_order["ExchangeID"],one_order["h_nano"],"204")
            LOG.write("\n"+'\n')
        else: # receive msg 207
            print(207)
            one_order = data_207.iloc[iter_207].copy()
            cancel_id = one_order["OrderRef"]
            # print("cancel id = ",cancel_id)
            iter_207 += 1
            if(cancel_id in not_canceled):
                print("cancel error",cancel_id)
                continue
            find = False
            # one_order["OrderRef"]
            # search in the orderbook_buy_line
            for i in range(0,len(orderbook_buy_line)):
                if find:
                    break
                if( orderbook_buy_line.iloc[i]["OrderRef"] == cancel_id ):
                    orderbook_buy_line.drop(orderbook_buy_line.index[i],inplace=True)
                    find = True
                    # print("find in the orderbook_buy_line")
                    break
            # search in the orderbook_sell_line
            for i in range(0,len(orderbook_sell_line)):
                if find:
                    break
                if( orderbook_sell_line.iloc[i]["OrderRef"] == cancel_id ):
                    # print(len(orderbook_sell_line))
                    # print(orderbook_sell_line["OrderRef"]) 
                    # print("end")
                    orderbook_sell_line.drop(orderbook_sell_line.index[i],inplace=True)
                    find = True
                    # print("find in the orderbook_sell_line")
                    break  
            if not find: 
                print("not find")
                continue     
            
            # print(len(orderbook_sell_line))
            # print(orderbook_sell_line["OrderRef"])    
            # print("end")   
            rtn_order = {"BrokerID":"","UserID":"","ParticipantID":"","InvestorID":"","BusinessUnit":"","InstrumentID":"","OrderRef":"","ExchangeID":"","LimitPrice":0,"VolumeTraded":0,"VolumeTotal":0,"VolumeTotalOriginal":0,"TimeCondition":"","VolumeCondition":"","OrderPriceType":"","Direction":"","OffsetFlag":"","HedgeFlag":"","OrderStatus":"","RequestID":0,"GatewayTime":"","h_nano":"" }
            rtn_order["BrokerID"] = one_order["BrokerID"]
            rtn_order["InvestorID"] = one_order["InvestorID"]
            rtn_order["InstrumentID"] = one_order["InstrumentID"]
            rtn_order["OrderRef"] = one_order["OrderRef"]
            rtn_order["ExchangeID"] = "MOCK"
            rtn_order["LimitPrice"] = one_order["LimitPrice"]
            rtn_order["OrderStatus"] = "Canceled"
            rtn_order["RequestID"] = 1
            rtn_order["h_nano"] = one_order["h_nano"]
            # print(rtn_order)
            writer_205.writerow(list(rtn_order.values()))
            LOG.write("\n 207 order_ref"+str(rtn_order["OrderRef"] ))
            write_orderbook(one_order["InstrumentID"],one_order["ExchangeID"],one_order["h_nano"],"207")


    


    




"""
data_106['h_nano'][]
data_105['h_nano'][]
data_204['h_nano'][]
InstrumentID = df['InstrumentID'][0]
print(InstrumentID)
"""
