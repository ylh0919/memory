import pandas as pd
import csv

"""
将td产生的 mock_106.csv mock_205.csv mock_206.csv 改名为 mock_106_C.csv mock_205_C.csv mock_206_C.csv
需要更改mock_106_C的抬头
InstrumentID,ExchangeID,UpdateMicroSecond,BidLevelCount,AskLevelCount,BidLevels,AskLevels,Status,h_extra_nano,h_nano,h_msg_type,h_request_id,h_source,h_is_last,h_error_id,j_name
需要更改mock_205_C的抬头
BrokerID,UserID,ParticipantID,InvestorID,BusinessUnit,InstrumentID,OrderRef,ExchangeID,LimitPrice,VolumeTraded,VolumeTotal,VolumeTotalOriginal,TimeCondition,VolumeCondition,OrderPriceType,Direction,OffsetFlag,HedgeFlag,OrderStatus,RequestID,GatewayTime,h_extra_nano,h_nano,h_msg_type,h_request_id,h_source,h_is_last,h_error_id,j_name
需要更改mock_206_C的抬头
BrokerID,UserID,InvestorID,BusinessUnit,InstrumentID,OrderRef,ExchangeID,TradeID,OrderSysID,ParticipantID,ClientID,Price,Volume,TradingDay,TradeTime,Direction,OffsetFlag,HedgeFlag,h_extra_nano,h_nano,h_msg_type,h_request_id,h_source,h_is_last,h_error_id,j_name
"""

# mock_206 & mock_206_C
file_206 = pd.read_csv('mock_206.csv')
data_206 = pd.DataFrame(file_206) 
size_206 = len(data_206)

file_206_C = pd.read_csv('mock_206_C.csv')
data_206_C = pd.DataFrame(file_206_C) 
size_206_C = len(data_206_C)

if(size_206 != size_206_C):
    print("error: 206 file length")
min_size = min(size_206,size_206_C)
for i in range(0,min_size):   
    rtn_order = data_206.iloc[i].copy()
    rtn_order_C = data_206_C.iloc[i].copy()
    if(rtn_order["OrderRef"] != rtn_order_C["OrderRef"]):
        # print(rtn_order["OrderRef"] , " " , rtn_order_C["OrderRef"])
        print("error: line ",i," OrderRef")
        continue
    elif(rtn_order["Price"] != rtn_order_C["Price"]):
        print("error: line ",i," Price")
        continue
    elif(rtn_order["Volume"] != rtn_order_C["Volume"]):
        print("error: line ",i," ,Volume")
        continue
    elif(rtn_order["Direction"] != rtn_order_C["Direction"]):
        print("error: line ",i," ,Direction")
        continue 
print("check 206 file --- end")

# mock_205 & mock_205_C
file_205 = pd.read_csv('mock_205.csv')
data_205 = pd.DataFrame(file_205) 
size_205 = len(data_205)

file_205_C = pd.read_csv('mock_205_C.csv')
data_205_C = pd.DataFrame(file_205_C) 
size_205_C = len(data_205_C)

if(size_205 != size_205_C):
    print("error: 205 file length")
min_size = min(size_205,size_205_C)
for i in range(0,min_size):   
    rtn_order = data_205.iloc[i].copy()
    rtn_order_C = data_205_C.iloc[i].copy()
    if(rtn_order["OrderRef"] != rtn_order_C["OrderRef"]):
        # print(rtn_order["OrderRef"] , " " , rtn_order_C["OrderRef"])
        print("error: line ",i," rtn_order: OrderRef:",rtn_order["OrderRef"])
        print("error: line ",i," rtn_order_C: OrderRef:",rtn_order_C["OrderRef"])
        continue
    elif(rtn_order["OrderStatus"] != rtn_order_C["OrderStatus"]):
        # print(rtn_order["OrderStatus"] , " " , rtn_order_C["OrderStatus"])
        print("error: line ",i," OrderStatus")
        continue
    elif(rtn_order["OrderStatus"] == "Canceled"):
        continue
    elif(rtn_order["LimitPrice"] != rtn_order_C["LimitPrice"]):
        print("error: line ",i," ,LimitPrice")
        continue
    elif(rtn_order["VolumeTraded"] != rtn_order_C["VolumeTraded"]):
        print("error: line ",i," ,VolumeTraded")
        continue 
    elif(rtn_order["Direction"] != rtn_order_C["Direction"]):
        print("error: line ",i," ,Direction")
        continue 
print("check 205 file --- end")

# 比较文件
# mock_106 & mock_106_C
# 比较内容
# BidLevelCount AskLevelCount BidLevels AskLevels

file_106 = pd.read_csv('mock_106.csv')
data_106 = pd.DataFrame(file_106) 
size_106 = len(data_106)

file_106_C = pd.read_csv('mock_106_C.csv')
data_106_C = pd.DataFrame(file_106_C) 
size_106_C = len(data_106_C)

if(size_106 != size_106_C):
    print("error: 106 file length")
min_size = min(size_106,size_106_C)
for i in range(0,min_size):
    # print(i)
    orderbook = data_106.iloc[i].copy()
    orderbook_C = data_106_C.iloc[i].copy()
    if(orderbook["BidLevelCount"] != orderbook_C["BidLevelCount"]):
        print("error: line",i,", BidLevelCount")
        print(orderbook["BidLevelCount"])
        print(orderbook_C["BidLevelCount"])
        # exit(0)
        continue
    if(orderbook["AskLevelCount"] != orderbook_C["AskLevelCount"]):
        print("error: line",i,", AskLevelCount")
        # exit(0)
        continue
    orderbook["BidLevels"] = eval(orderbook["BidLevels"])
    orderbook["BidLevels"]["volume"] = list(orderbook["BidLevels"]["volume"])
    orderbook["AskLevels"] = eval(orderbook["AskLevels"])
    orderbook["AskLevels"]["volume"] = list(orderbook["AskLevels"]["volume"])
    # print(orderbook["AskLevels"]["volume"][0])
    list1 = orderbook_C["BidLevels"].split(";")
    orderbook_C["BidLevels"] = {"volume":[],"price":[]}
    for volume_price in list1:
        if(volume_price == ""): continue
        list2 = volume_price.split("@")
        orderbook_C["BidLevels"]["volume"].append(list2[0])
        orderbook_C["BidLevels"]["price"].append(list2[1])
    list1 = orderbook_C["AskLevels"].split(";")
    orderbook_C["AskLevels"] = {"volume":[],"price":[]}
    for volume_price in list1:
        if(volume_price == ""): continue
        list2 = volume_price.split("@")
        orderbook_C["AskLevels"]["volume"].append(list2[0])
        orderbook_C["AskLevels"]["price"].append(list2[1])
    k = min(10,orderbook["BidLevelCount"])
    for j in range(0,k):
        if( orderbook["BidLevels"]["price"][j] != int(orderbook_C["BidLevels"]["price"][j]) ):
            # print(orderbook["BidLevels"]["price"][j]," ",orderbook_C["BidLevels"]["price"][j])
            # print(type(orderbook["BidLevels"]["price"][j])," ",type(orderbook_C["BidLevels"]["price"][j]))
            print("error: line ",i,", BidLevels ",j,",price")
            # exit(0)
            continue
        if( orderbook["BidLevels"]["volume"][j] != int(orderbook_C["BidLevels"]["volume"][j]) ):
            print(orderbook["BidLevels"]["volume"][j]," ",orderbook_C["BidLevels"]["volume"][j])
            print("error: line ",i,", BidLevels ",j,",volume")
            # exit(0)
            continue    
    k = min(0,orderbook["AskLevelCount"]) 
    for j in range(0,k):
        if( orderbook["AskLevels"]["price"][j] != int(orderbook_C["AskLevels"]["price"][j]) ):
            print("error: line ",i,", AskLevels ",j,",price")
            # exit(0)
            continue
        if( orderbook["AskLevels"]["volume"][j] != int(orderbook_C["AskLevels"]["volume"][j]) ):
            print("error: line ",i,", AskLevels ",j,",volume")
            # exit(0)
            continue         
print("check 106 file --- end")





