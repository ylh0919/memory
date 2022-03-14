#coding=utf-8
import functools, random, sys
import csv
import numpy as np
import time
import datetime
import os
 
np.set_printoptions(suppress=True)
bitflyer_data = []
bitflyer_inverse_data = []
coinflex_data = []
coinflex_time = 0
bitflyer_time = 0
bitflyer_BidLevelCount = 0
bitflyer_AskLevelCount = 0
coinflex_BidLevelCount = 0
coinflex_AskLevelCount = 0
D = []
I = []
N = []
Q = []
V = []
W = []
X = []
Y = []
Z = []
AC = []

today=''
yest=''
path=''
path_jpy_usd=''
path_bitflyer=''
path_coinflex=''


def writefile1(timea):
    global yest,path,path_bitflyer,path_coinflex,path_jpy_usd
    timea /= 10**9
    time_local = time.localtime(int(timea))     
    dt = time.strftime("%Y-%m-%d",time_local)
    if yest != dt:
        yest = dt
        path = "/shared/jpy_usd_coinflex/"+dt
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            path_bitflyer = path+'/md_bitflyer.csv'
            path_jpy_usd = path+'/jpy_usd_coinflex.csv'
            path_coinflex = path+'/md_coinflex.csv'
            return
        path_bitflyer = path+'/md_bitflyer.csv'
        path_jpy_usd = path+'/jpy_usd_coinflex.csv'
        path_coinflex = path+'/md_coinflex.csv'

        with open(path_jpy_usd,'w') as f:
            csv_write = csv.writer(f)
            csv_head = ["InstrumentID","coinflex_time","bitflyer_time","coinflex_nano","bitflyer_nano"]
            for i in range(0,20):
                csv_head.append("BidVol"+str(i))
                csv_head.append("BidPrice"+str(i))
            for i in range(0,20):
                csv_head.append("AskPrice"+str(i))
                csv_head.append("AskVol"+str(i))
            csv_write.writerow(csv_head)

        with open(path_coinflex,'w') as f:
            csv_write = csv.writer(f)
            csv_head = ["InstrumentID","coinflex_time","coinflex_nano","BidLevelCount","AskLevelCount"]
            for i in range(0,20):
                csv_head.append("BidVol["+str(i)+"]")
                csv_head.append("BidPrice["+str(i)+"]")
            for i in range(0,20):
                csv_head.append("AskPrice["+str(i)+"]")
                csv_head.append("AskVol["+str(i)+"]")
            csv_write.writerow(csv_head)

        with open(path_bitflyer,'w') as f:
            csv_write = csv.writer(f)
            csv_head = ["InstrumentID","bitflyer_time","bitflyer_nano","BidLevelCount","AskLevelCount"]
            for i in range(0,20):
                csv_head.append("BidVol["+str(i)+"]")
                csv_head.append("BidPrice["+str(i)+"]")
            for i in range(0,20):
                csv_head.append("AskPrice["+str(i)+"]")
                csv_head.append("AskVol["+str(i)+"]")
            csv_write.writerow(csv_head)

def timestamp_tran_time(timestamp):
    ini_time = str(datetime.datetime.utcfromtimestamp(timestamp // 10**9))
    dif_timestamp_ms = timestamp - timestamp // 10**9 * 10**9
    ini_time_ms = dif_timestamp_ms // 10**6
    ini_time = ini_time + ":"+ str(ini_time_ms)
    dif_timestamp_us = dif_timestamp_ms - dif_timestamp_ms // 10**6 * 10**6
    ini_time_us = dif_timestamp_us // 10**3
    ini_time = ini_time + ":"+ str(ini_time_us)
    ini_time_ns = dif_timestamp_us - dif_timestamp_us // 10**3 * 10**3
    ini_time = ini_time + ":" + str(ini_time_ns)
    return ini_time


def initialize(context):
    context.white_list = {}
    context.complete_list = {}
    context.order_status = {}
    context.maker_status = {}
    context.top_of_book = {}
    import os
    if "config" in os.environ:
        import json
        with open(os.environ["config"]) as f:
            data = json.load(f)
            
            invalid_exch = False 
            if "exchange" in data:
                exch_src = get_source_from_exchange(data["exchange"])
                if not exch_src:
                    invalid_exch = True
                else:
                    context.exch_src = exch_src
                    context.exch_id = str(data["exchange"])
            else:
                invalid_exch = True
            
            if invalid_exch:            
                raise Exception("invalid exchange in config file")

            if "tickers" in data:
                for ticker, ticker_conf in data["tickers"].items():
                     interval = int(ticker_conf["interval_seconds"])
                     total_time = int(ticker_conf["total_time_seconds"])
                     iterations = int(total_time / interval)
                     total_volume = scale_volume(int(ticker_conf["total_volume"]))
                     unit_volume = int(total_volume / iterations)
                     side_str = ticker_conf["side"]
                     side = None
                     if side_str in ("buy", "sell"):
                         side = DIRECTION.Buy if side_str == "buy" else DIRECTION.Sell
                    
                     method_str = ticker_conf["method"]
                     method = None
                     if method_str in ("taker", "maker"):
                         method = 0 if method_str == "taker" else 1

                     if interval > 0 and iterations > 0 and unit_volume > 0 and side != None and method != None and total_volume >= unit_volume:
                        context.white_list[str(ticker)] = [interval, unit_volume, total_volume, side, method, 0, 0]
                        #order_id, target_qty, traded_qty, side, target_price
                        context.order_status[str(ticker)] = [-1, 0, 0, None, None]

                        if method == 1:
                            # 0 = init, 1 = in_make, 2 = in_take
                            # remaining target qty in the current maker iteration
                            context.maker_status[str(ticker)] = [0, None, False]
                            context.top_of_book[str(ticker)] = top_of_book(None, None)

                            config = context.white_list[str(ticker)]
                            config[5] = int(ticker_conf["maker_interval_seconds"])
                            
                            if config[0] <= config[5]:
                                print_log(context, "invalid config, total_interval {} less than maker_interval {}".format(config[0], config[5]))
                                sys.exit(1)

                            config[6] = float(ticker_conf["maker_price_offset_pct"])
                     else:
                         print_log(context, "invalid config for {}".format(ticker))
                         sys.exit(1)

    context.add_md(source=SOURCE.COINFLEX)
    context.add_md(source=SOURCE.BITFLYER)
    # context.add_td(source=SOURCE.COINFLEX)
    # context.add_td(source=SOURCE.BITFLYER)
    context.subscribe(tickers=['btc_usd'], source=SOURCE.COINFLEX)
    context.subscribe(tickers=['fx_btc_jpy'], source=SOURCE.BITFLYER)

    print context.white_list
    
def on_price_book(context, price_book, source, rcv_time):
    if source != SOURCE.COINFLEX and source != SOURCE.BITFLYER:
        return
    global path_bitflyer,path_coinflex,path_jpy_usd
    global bitflyer_data,bitflyer_inverse_data,coinflex_data,coinflex_time,bitflyer_time,D,I,N,Q,V,W,X,Y,Z,AC,path,bitflyer_BidLevelCount,bitflyer_AskLevelCount,coinflex_BidLevelCount,coinflex_AskLevelCount
    if price_book.InstrumentID in ['fx_btc_jpy','btc_usd']:
        if price_book.InstrumentID == "fx_btc_jpy":
            bitflyer_data = []
            bitflyer_inverse_data = []
            N = []
            Q = []
            bitflyer_time = rcv_time
            bitflyer_BidLevelCount = price_book.BidLevelCount
            bitflyer_AskLevelCount = price_book.AskLevelCount
            for i in range(0,price_book.BidLevelCount):
                bitflyer_data.append(float(price_book.BidLevels.levels[i].volume)/10**8)
                bitflyer_data.append(float(price_book.BidLevels.levels[i].price)/10**8)
            for i in range(price_book.BidLevelCount,20):
                bitflyer_data.append(0)
                bitflyer_data.append(0)
            for i in range(0,price_book.AskLevelCount):
                bitflyer_data.append(float(price_book.AskLevels.levels[i].price)/10**8)
                bitflyer_data.append(float(price_book.AskLevels.levels[i].volume)/10**8)
            for i in range(price_book.AskLevelCount,20):
                bitflyer_data.append(0)
                bitflyer_data.append(0)
            #求逆
            for i in range(0,20):
                bitflyer_inverse_data.append(float(bitflyer_data[40+2*i]*bitflyer_data[41+2*i]))
                if bitflyer_data[40+2*i] == 0:
                    bitflyer_inverse_data.append(0)
                else:
                    bitflyer_inverse_data.append(float(1.0/bitflyer_data[40+2*i]))
                N.append(float(bitflyer_data[40+2*i]*bitflyer_data[41+2*i]*1.0/bitflyer_data[40+2*i]))
            for i in range(0,20):
                if bitflyer_data[2*i+1] == 0:
                    bitflyer_inverse_data.append(0)
                else:
                    bitflyer_inverse_data.append(float(1.0/bitflyer_data[2*i+1]))
                bitflyer_inverse_data.append(float(bitflyer_data[2*i]*bitflyer_data[2*i+1]))
                Q.append(float(bitflyer_data[2*i]*bitflyer_data[2*i+1]*1.0/bitflyer_data[2*i+1]))
            if coinflex_data != []:
                W = []
                X = []
                Y = []
                Z = []
                V = []
                level_a = D[0]
                level_b = N[0]
                level_c = I[0]
                level_d = Q[0]
                temp_a = 0
                temp_b = 0
                temp_c = 0
                temp_d = 0
                for i in range(0,20):
                    flag1 = 0
                    flag2 = 0
                    if level_a == 0:
                        temp_a += 1
                        flag1 = 1
                    if level_b == 0:   
                        temp_b += 1
                        flag2 = 1 
                    if flag1 == 1:
                        level_a = float(D[temp_a])
                    if flag2 == 1:
                        level_b = float(N[temp_b])
                    if level_a == 0 or level_b == 0:
                        break 
                    bidusdvolume = min(level_a,level_b)
                    level_a -= bidusdvolume
                    level_b -= bidusdvolume     
                    W.append('%.f'%(bidusdvolume*10**8/float(bitflyer_inverse_data[1+temp_b*2])))
                    X.append('%.f'%(10**8*float(coinflex_data[1+temp_a*2])*float(bitflyer_inverse_data[1+temp_b*2])))
                #    print(float(bitflyer_inverse_data[1+temp_b*2]))
                for i in range(0,20):
                    flag3 = 0
                    flag4 = 0
                    if level_c == 0:
                        temp_c += 1
                        flag3 = 1
                    if level_d == 0:   
                        temp_d += 1
                        flag4 = 1 
                    if flag3 == 1:
                        level_c = float(I[temp_c])
                    if flag4 == 1:
                        level_d = float(Q[temp_d])
                    if level_c == 0 or level_d == 0:
                        break 
                    askusdvolume = min(level_c,level_d)
                    level_c -= askusdvolume
                    level_d -= askusdvolume              
                    Y.append('%.f'%(10**8*float(coinflex_data[40+temp_c*2])*float(bitflyer_inverse_data[40+temp_d*2])))
                    Z.append('%.f'%(10**8*askusdvolume/float(bitflyer_inverse_data[40+temp_d*2])))
                
                writefile1(coinflex_time)
                with open(path_jpy_usd,'a+') as f:
                    csv_write = csv.writer(f)
                    data_row = ["jpy_usd"]
                    data_row.append(timestamp_tran_time(coinflex_time))
                    data_row.append(timestamp_tran_time(bitflyer_time))
                    data_row.append(coinflex_time)
                    data_row.append(bitflyer_time)
                    for i in range(0,len(W)):
                        data_row.append(float(W[i])/10**8)
                        data_row.append(float(X[i])/10**8)
                    for i in range(0,len(Y)):
                        data_row.append(float(Y[i])/10**8)
                        data_row.append(float(Z[i])/10**8)
                    csv_write.writerow(data_row)
                    print("coinflex_bitflyer_jpy_usd_f")
                    print(data_row)

                with open(path_coinflex,'a+') as f:
                    csv_write = csv.writer(f)
                    data_row = ["btc_usd"]
                    data_row.append(timestamp_tran_time(coinflex_time))
                    data_row.append(coinflex_time)
                    data_row.append(coinflex_BidLevelCount)
                    data_row.append(coinflex_AskLevelCount)
                    for i in range(0,80):
                        data_row.append(coinflex_data[i])
                    csv_write.writerow(data_row)
                with open(path_bitflyer,'a+') as f:
                    csv_write = csv.writer(f)
                    data_row = ["fx_btc_jpy"]
                    data_row.append(timestamp_tran_time(bitflyer_time))
                    data_row.append(bitflyer_time)
                    data_row.append(bitflyer_BidLevelCount)
                    data_row.append(bitflyer_AskLevelCount)
                    for i in range(0,80):
                        data_row.append(bitflyer_data[i])
                    csv_write.writerow(data_row)

                D = []
                I = []
                N = []
                Q = []
                V = []
                W = []
                X = []
                Y = []
                Z = []
                AC = []

                bitflyer_data = []
                coinflex_data = []
        elif price_book.InstrumentID == "btc_usd":
            D = []
            I = []
            coinflex_data = []
            coinflex_time = rcv_time
            coinflex_BidLevelCount = price_book.BidLevelCount
            coinflex_AskLevelCount = price_book.AskLevelCount
            for i in range(0,price_book.BidLevelCount):
                coinflex_data.append(float(price_book.BidLevels.levels[i].volume)/10**8)
                coinflex_data.append(float(price_book.BidLevels.levels[i].price)/10**8)
                D.append(float(price_book.BidLevels.levels[i].volume)/10**8)
            for i in range(price_book.BidLevelCount,20):
                coinflex_data.append(0)
                coinflex_data.append(0)
                D.append(0)
            for i in range(0,price_book.AskLevelCount):
                coinflex_data.append(float(price_book.AskLevels.levels[i].price)/10**8)
                coinflex_data.append(float(price_book.AskLevels.levels[i].volume)/10**8)
                I.append(float(price_book.AskLevels.levels[i].volume)/10**8)
            for i in range(price_book.AskLevelCount,20):
                coinflex_data.append(0)
                coinflex_data.append(0)
                I.append(0)
            if bitflyer_data != []:
                W = []
                X = []
                Y = []
                Z = []
                V = []
                level_a = D[0]
                level_b = N[0]
                level_c = I[0]
                level_d = Q[0]
                temp_a = 0
                temp_b = 0
                temp_c = 0
                temp_d = 0
                for i in range(0,20):
                    flag1 = 0
                    flag2 = 0
                    if level_a == 0:
                        temp_a += 1
                        flag1 = 1
                    if level_b == 0:   
                        temp_b += 1
                        flag2 = 1 
                    if flag1 == 1:
                        level_a = float(D[temp_a])
                    if flag2 == 1:
                        level_b = float(N[temp_b])
                    if level_a == 0 or level_b == 0:
                        break 
                    bidusdvolume = min(level_a,level_b)
                    level_a -= bidusdvolume
                    level_b -= bidusdvolume        
                    W.append('%.f'%(bidusdvolume*10**8/float(bitflyer_inverse_data[1+temp_b*2])))
                    X.append('%.f'%(10**8*float(coinflex_data[1+temp_a*2])*float(bitflyer_inverse_data[1+temp_b*2])))
                for i in range(0,20):
                    flag3 = 0
                    flag4 = 0
                    if level_c == 0:
                        temp_c += 1
                        flag3 = 1
                    if level_d == 0:   
                        temp_d += 1
                        flag4 = 1 
                    if flag3 == 1:
                        level_c = float(I[temp_c])
                    if flag4 == 1:
                        level_d = float(Q[temp_d])
                    if level_c == 0 or level_d == 0:
                        break 
                    askusdvolume = min(level_c,level_d)
                    level_c -= askusdvolume
                    level_d -= askusdvolume              
                    Y.append('%.f'%(10**8*float(coinflex_data[40+temp_c*2])*float(bitflyer_inverse_data[40+temp_d*2])))
                    Z.append('%.f'%(10**8*askusdvolume/float(bitflyer_inverse_data[40+temp_d*2])))

                writefile1(coinflex_time)
                with open(path_jpy_usd,'a+') as f:
                    csv_write = csv.writer(f)
                    data_row = ["jpy_usd"]
                    data_row.append(timestamp_tran_time(coinflex_time))
                    data_row.append(timestamp_tran_time(bitflyer_time))
                    data_row.append(coinflex_time)
                    data_row.append(bitflyer_time)
                    for i in range(0,len(W)):
                        data_row.append(float(W[i])/10**8)
                        data_row.append(float(X[i])/10**8)
                    for i in range(0,len(Y)):
                        data_row.append(float(Y[i])/10**8)
                        data_row.append(float(Z[i])/10**8)
                    csv_write.writerow(data_row)
                    print("coinflex_bitflyer_jpy_usd_f")
                    print(data_row)
                with open(path_coinflex,'a+') as f:
                    csv_write = csv.writer(f)
                    data_row = ["btc_usd"]
                    data_row.append(timestamp_tran_time(coinflex_time))
                    data_row.append(coinflex_time)
                    data_row.append(coinflex_BidLevelCount)
                    data_row.append(coinflex_AskLevelCount)
                    for i in range(0,80):
                        data_row.append(coinflex_data[i])
                    csv_write.writerow(data_row)
                with open(path_bitflyer,'a+') as f:
                    csv_write = csv.writer(f)
                    data_row = ["fx_btc_jpy"]
                    data_row.append(timestamp_tran_time(bitflyer_time))
                    data_row.append(bitflyer_time)
                    data_row.append(bitflyer_BidLevelCount)
                    data_row.append(bitflyer_AskLevelCount)
                    for i in range(0,80):
                        data_row.append(bitflyer_data[i])
                    csv_write.writerow(data_row)
                D = []
                I = []
                N = []
                Q = []
                V = []
                W = []
                X = []
                Y = []
                Z = []
                AC = []

                coinflex_data = []
                bitflyer_data = []

