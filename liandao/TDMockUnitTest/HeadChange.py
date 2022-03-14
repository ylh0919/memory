import os
import csv


os.sys.argv[1]
file_106 = csv.reader(open(os.sys.argv[1], 'r'))



head_106 = os.sys.argv[2]
head_205 = "BrokerID,UserID,ParticipantID,InvestorID,BusinessUnit,InstrumentID,OrderRef,ExchangeID,LimitPrice,VolumeTraded,VolumeTotal,VolumeTotalOriginal,TimeCondition,VolumeCondition,OrderPriceType,Direction,OffsetFlag,HedgeFlag,OrderStatus,RequestID,GatewayTime,h_extra_nano,h_nano,h_msg_type,h_request_id,h_source,h_is_last,h_error_id,j_name"
heas_206 = "BrokerID,UserID,InvestorID,BusinessUnit,InstrumentID,OrderRef,ExchangeID,TradeID,OrderSysID,ParticipantID,ClientID,Price,Volume,TradingDay,TradeTime,Direction,OffsetFlag,HedgeFlag,h_extra_nano,h_nano,h_msg_type,h_request_id,h_source,h_is_last,h_error_id,j_name"

rows_106 = []
rows_205 = []
rows_206 = []


for x in file_106:
    rows_106.append(x)
   # print(x)

#print(rows_106)
rows_106[0] = [x for x in head_106.split(",")]


writer_106 = csv.writer(open(os.sys.argv[1], 'w', newline=''))
writer_106.writerows(rows_106)


