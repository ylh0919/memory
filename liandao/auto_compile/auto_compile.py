import os
import json
with open("template.json",'r') as load_f:
    template = json.load(load_f)
with open("kungfu_sample_template.json",'r') as load_f:
    kungfu_sample_template = json.load(load_f)

with open("exchange_select.json","r") as load_f:
    exchange_select = json.load(load_f)["select"]
if "XTP" not in exchange_select:
    exchange_select.append("XTP")
if "CTP" not in exchange_select:
    exchange_select.append("CTP")
size = len(exchange_select) # the number of the exchanges you choose    

exchange = {'CTP':{'source_index':1},
            'SSE':{'source_index':1},
            'SZE':{'source_index':2},
            'CFFEX':{'source_index':11},
            'SHFE':{'source_index':12}, 
            'DCE':{'source_index':13},
            'CZCE':{'source_index':14},
            'XTP':{'source_index':15},
            'BINANCE':{'source_index':16},
            'INDODAX':{'source_index':17} ,
            'OKEX':{'source_index':18},
            'COINMEX':{'source_index':19},
            'MOCK':{'source_index':20},
            'BITMAX':{'source_index':21},
            'BITFINEX':{'source_index':22},
            'BITMEX':{'source_index':23},
            'HITBTC':{'source_index':24},
            'OCEANEX':{'source_index':25},
            'HUOBI':{'source_index':26},
            'OCEANEXB':{'source_index':27},
            'PROBIT':{'source_index':28},
            'BITHUMB':{'source_index':29},
            'UPBIT':{'source_index':30},
            'DAYBIT':{'source_index':31},
            'KUCOIN':{'source_index':32},
            'BITFLYER':{'source_index':33},
            'KRAKEN':{'source_index':34},
            'IB':{'source_index':35},
            'BITTREX':{'source_index':36},
            'POLONIEX':{'source_index':37},
            'BITSTAMP':{'source_index':38},
            'DERIBIT':{'source_index':39},
            'EMX':{'source_index':40},
            'COINFLEX':{'source_index':41},
            'COINFLOOR':{'source_index':42},
            'MOCKKUCOIN':{'source_index':43},
            'MOCKBITMEX':{'source_index':44},
            'ERISX':{'source_index':45},
            'HBDM':{'source_index':46},
            'KUMEX':{'source_index':47},
            'BINANCEF':{'source_index':48},
            'BINANCE2':{'source_index':49}
            }
# print(exchange['EMX']['source_index'])

def generate0(): 
    str0 = ""
    for i in range(size):
        str0 += template["0"].format(exchange_select[i],exchange[exchange_select[i]]['source_index'])
        if( i < size-1 ):
             str0 += ','
        str0 += "\n"
    return str0

def generate1():
    str1 = ""
    for i in range(size):
        str1 += template["1"].format(exchange_select[i],exchange_select[i].lower())
        if( i < size-1 ):
             str1 += ','
        str1 += "\n"
    return str1

def generate2():
    str2 = ""
    for i in range(size):
        str2 += template["2"].format(exchange_select[i])
        str2 += "\n"
    return str2

def generate3():
    str3 = ""
    for i in range(size):
        str3 += template["3"].format(exchange_select[i],exchange[exchange_select[i]]['source_index'])
        str3 += "\n"
    return str3

def generate4():
    str4 = ""
    for i in range(size):
        str4 += template["4"].format(exchange_select[i],exchange[exchange_select[i]]['source_index'])
        str4 += "\n\n"
    return str4

def generate5():
    str5 = ""
    for i in range(size):
        str5 += template["5"].format(exchange_select[i])
        str5 += "\n"
    return str5

def generate6():
    str6 = ""
    for i in range(size):
        str6 += template["6"].format(exchange_select[i])
        str6 += "\n"
    return str6    

def generate7():
    str7 = ""
    for i in range(size):
        str7 += template["7"].format(exchange_select[i],exchange[exchange_select[i]]['source_index'])
        str7 += "\n"
    return str7    

def generate8():
    str8 = ""
    for i in range(size):
        str8 += template["8"].format(exchange_select[i])
        str8 += "\n"
    return str8    

def generate9():
    str9 = ""
    for i in range(size):
        str9 += template["9"].format(exchange_select[i],exchange[exchange_select[i]]['source_index'])
        str9 += "\n"
    return str9

def generate10():
    str10 = ""
    for i in range(size):
        str10 += template["10"].format(exchange_select[i])
    return str10

def generate11():
    str11 = ""
    for i in range(size):
        str11 += template["11"].format(exchange_select[i],exchange[exchange_select[i]]['source_index'])
        if( i < size-1 ): 
            str11 += ","
        str11 += "\n                       "
    return str11

def generate_kungfu_sample():
    # the beginning
    jsonSmaple={}
    # monitor
    jsonSmaple["monitor"] = kungfu_sample_template["monitor"]
    # md
    jsonSmaple["md"]={}
    for i in range(size):
        jsonSmaple["md"][exchange_select[i].lower()] = kungfu_sample_template["md"][exchange_select[i].lower()]
    # td
    jsonSmaple["td"]={}
    for i in range(size):
        jsonSmaple["td"] = kungfu_sample_template["td"][exchange_select[i].lower()]
    return jsonSmaple

def generate12():
    str12 = ""
    for i in range(size):
        str12 += template["12"].format(exchange_select[i])
        str12 += "\n"
    return str12

def generate_md_CMakeLists():
    strs = ""
    for i in range(size):
        strs += template["md_cmakelists_"+exchange_select[i]]
        strs += "\n"
    return strs

def generate_td_CMakeLists():
    strs = ""
    for i in range(size):
        strs += template["td_cmakelists_"+exchange_select[i]]
        strs += "\n"
    return strs

def generate13():
    str13 = ""
    for i in range(size):
        str13 += template["13"].format(exchange_select[i])
        str13 += "\n"
    return str13

if __name__ == "__main__":
    # longfist\longfist\LFConstants.h
    if(os.path.exists("../longfist/longfist/LFConstants.h")):
        os.remove("../longfist/longfist/LFConstants.h")
    read_file = open("LFConstants_template.h",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../longfist/longfist/LFConstants.h",'w', encoding="utf-8")
    write_file.write(str.format(generate0(),generate1(),generate2(),generate3(),generate4()))
    write_file.close()
    # longfist\longfist\LFUtils.h
    if(os.path.exists("../longfist/longfist/LFUtils.h")):
        os.remove("../longfist/longfist/LFUtils.h")
    read_file = open("LFUtils_template.h",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../longfist/longfist/LFUtils.h",'w', encoding="utf-8")
    write_file.write(str.format(generate5(),generate6()))
    write_file.close()
    # python/kungfu/longfist/longfist_constants.py
    if(os.path.exists("../python/kungfu/longfist/longfist_constants.py")):
        os.remove("../python/kungfu/longfist/longfist_constants.py")
    read_file = open("longfist_constants_template.py",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../python/kungfu/longfist/longfist_constants.py",'w', encoding="utf-8")
    write_file.write(str.format(generate7(),generate8(),generate9()))
    write_file.close()
    # python/kungfu/longfist/longfist_structs.py
    if(os.path.exists("../python/kungfu/longfist/longfist_structs.py")):
        os.remove("../python/kungfu/longfist/longfist_structs.py")
    read_file = open("longfist_structs_template.py",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../python/kungfu/longfist/longfist_structs.py",'w', encoding="utf-8")
    write_file.write(str.format(generate10()))
    write_file.close()
    # python/kungfu/wingchun/constants.py
    if(os.path.exists("../python/kungfu/wingchun/constants.py")):
        os.remove("../python/kungfu/wingchun/constants.py")
    read_file = open("constants_template.py",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../python/kungfu/wingchun/constants.py",'w', encoding="utf-8")
    write_file.write(str.format(generate7(),generate8()))
    write_file.close()
    # python/kungfu/wingchun/wc_configs.py
    if(os.path.exists("../python/kungfu/wingchun/wc_configs.py")):
        os.remove("../python/kungfu/wingchun/wc_configs.py")    
    read_file = open("wc_configs_template.py",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../python/kungfu/wingchun/wc_configs.py",'w', encoding="utf-8")
    write_file.write(str.format(generate11()))
    write_file.close()
    # rpm/etc/kungfu/kungfu.json.sample
    if(os.path.exists("../rpm/etc/kungfu/kungfu.json.sample")):
        os.remove("../rpm/etc/kungfu/kungfu.json.sample")  
    write_file = open("../rpm/etc/kungfu/kungfu.json.sample",'w', encoding="utf-8")
    json.dump(generate_kungfu_sample(),write_file,indent=4)
    # rpm/etc/supervisor/conf.d/md_bitfinex.conf  
    # remove md_* and td_* files
    path = "../rpm/etc/supervisor/conf.d/"
    for root,dirs,files in os.walk(path):
        for names in files :
            if (names.startswith("md_") or names.startswith("td_")) :
                os.remove( path+names )
    read_file = open("md_exchange_template.conf",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    for i in range(size):
        write_file = open("../rpm/etc/supervisor/conf.d/md_"+exchange_select[i].lower()+".conf",'w', encoding="utf-8")
        write_file.write(str.format(exchange_select[i].lower()))
        write_file.close()
    # rpm/etc/supervisor/conf.d/td_bitfinex.conf
    read_file = open("td_exchange_template.conf",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    for i in range(size):
        write_file = open("../rpm/etc/supervisor/conf.d/td_"+exchange_select[i].lower()+".conf",'w', encoding="utf-8")
        write_file.write(str.format(exchange_select[i].lower()))
        write_file.close()
    # rpm/scripts/post_install.sh
    if(os.path.exists("../rpm/scripts/post_install.sh")):
        os.remove("../rpm/scripts/post_install.sh")      
    read_file = open("post_install_template.sh",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../rpm/scripts/post_install.sh",'w', encoding="utf-8")
    write_file.write(str.format(generate12()))
    write_file.close()
    # wingchun/md/CMakeLists.txt
    if(os.path.exists("../wingchun/md/CMakeLists.txt")):
        os.remove("../wingchun/md/CMakeLists.txt")     
    read_file = open("md_CMakeLists_template.txt",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../wingchun/md/CMakeLists.txt",'w', encoding="utf-8")
    write_file.write(str.format(generate_md_CMakeLists()))
    write_file.close()
    # wingchun/td/CMakeLists.txt
    if(os.path.exists("../wingchun/td/CMakeLists.txt")):
        os.remove("../wingchun/td/CMakeLists.txt")      
    read_file = open("td_CMakeLists_template.txt",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../wingchun/td/CMakeLists.txt",'w', encoding="utf-8")
    write_file.write(str.format(generate_td_CMakeLists()))
    write_file.close()
    # yijinjing/journal/JournalFinder.cpp
    if(os.path.exists("../yijinjing/journal/JournalFinder.cpp")):
        os.remove("../yijinjing/journal/JournalFinder.cpp")   
    read_file = open("JournalFinder_template.cpp",encoding='UTF-8')
    str = read_file.read()
    read_file.close()
    write_file = open("../yijinjing/journal/JournalFinder.cpp",'w', encoding="utf-8")
    write_file.write(str.format(generate13()))
    write_file.close()