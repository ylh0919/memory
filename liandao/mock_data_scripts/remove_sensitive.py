import pandas as pd
import sys
import os
from loguru import logger 

#logger.add()


# 脚本作用为替换csv文件的敏感词
# 导出BinanceF和Upbit的一个星期的105，106，110, 219（将219处理为mock219格式），将以下关键字进行替换，得到anonymized_mock_data（都是mock MD和TD读取的csv），我们后面测试策略需要这一套Mock Data

# 脚本处理过的会改成 "_rsw.csv"的 后缀  标记已经完成敏感词替换

# 需要替换的敏感词
# BINANCE: ex02a
# UPBIT: ex12
# IB: ex13
# btc_usdt:token0002_token9002
# btc_krw:token0002_token9001
# krw_usd:token9001_token9006

# 105 106 110 219 中
# ExchangeID(c16)
# InstrumentID(c31)
# j_name(s)



instrument_map = {"xrp_usdt":"token0004_token9002", "ltc_usdt":"token0008_token9002", "eth_usdt":"token0001_token9002", "eth_usdt_211231":"token0001_token9002_211231", "eos_usdt":"token0005_token9002", "btc_usdt":"token0002_token9002", "btc_usdt_211231":"token0002_token9002_211231", "bnb_usdt":"token0009_token9002", "bch_usdt":"token0006_token9002"}

exchange_map = {"Binance Futures":"ex02a", "binancef":"ex02a"}

j_name_map = {"MD_BINANCEF":"MD_ex02a"}

# 查找所有不包含rsw的后缀的文件
def read_file_list(root_path):
    file_list = os.listdir(root_path)
    target_list = []

    for file_name in file_list:
        # 找到文件的绝对路径
        file_name = os.path.abspath(file_name)
        if not os.path.isdir(file_name):
            # 查找未处理过的csv文件
            if "rsw" not in file_name and "csv" in file_name:
                target_list.append(file_name)

    return target_list

def remove_sensitive_words(target_list):
    for data_path in (target_list):
        pd_chunk_list = pd.read_csv(data_path, chunksize=10000)
        print(data_path)
        df_list=[]
        for chunk in pd_chunk_list:
#            if "105" in data_path or "106" in data_path or "110" in data_path or "219" in data_path:
                chunk["ExchangeID(c16)"] = chunk["ExchangeID(c16)"].map(exchange_map)
                chunk["InstrumentID(c31)"] = chunk["InstrumentID(c31)"].map(instrument_map)
                chunk["j_name(s)"] = chunk["j_name(s)"].map(j_name_map)

#                chunk["exchange_id"] = chunk["exchange_id"].map(exchange_map)
#                chunk["instrument"] = chunk["instrument"].map(instrument_map)

                df_list.append(chunk)
#                print(chunk)
            # elif "106" in data_path:
            #     pass
            # elif "110" in data_path:
            #     pass
            # elif "219" in data_path:
            #     pass
        df = pd.concat(df_list, ignore_index=True)
        df.to_csv(data_path, index=False)
        print(len(df_list))
                

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('params error: The root directory must be entered')
    else:
        root_path = sys.argv[1]
        target_list = read_file_list(root_path)
        remove_sensitive_words(target_list)