import json
import time
import os


def unicode_convert(input):
    if isinstance(input, dict):
        return {unicode_convert(key): unicode_convert(value) for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [unicode_convert(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input


def json_has_key(json, key):
    if key in json:
        return True
    else:
        return False


def addToDict2(thedict, key_a, key_b, val):
    if key_a in thedict:
        thedict[key_a].update({key_b: val})
    else:
        thedict.update({key_a: {key_b: val}})


def addToDict3(thedict, key_b, key_c, val):
    if key_b in thedict[key_b]:
        thedict[key_b].update({key_c: val})
    else:
        thedict.update({key_b: {key_c: val}})


def reqpos(context):
    for key in transfer_thre_json:
        if type(key) == int and transfer_thre_json[key]["participating"] == 1:
            print(transfer_thre_json[key]["main_account"]["account_type"])
            print(transfer_thre_json[key]["main_account"]["account_name"])
            context.req_pos(source=key,
                            # req_pos(main account)
                            account_type=transfer_thre_json[key]["main_account"]["account_type"],
                            account_name=transfer_thre_json[key]["main_account"]["account_name"])
            for cur in transfer_thre_json[key]["lower_threshold"]:
                for acc in transfer_thre_json[key][cur]["default_account"]:

                    print(acc["account_type"])
                    print(acc["account_name"])
                    context.req_pos(source=key,
                                    # req_pos cur deafult_account
                                    account_type=acc["account_type"],
                                    account_name=acc["account_name"])
    context.insert_func_after_c(x=freq, y=reqpos)


# every freq,call reqpos once
def initialize(context):
    '''
    change transfer_threshold.json format
    {
  "balance_check_freq_ms": 10000,
  "batch_vol": {
    "xrp": 75,
    "pax": 2500,
    "tusd": 2500,
    "btc": 0.5,
    "eos": 1000
  },
  32: {
    "exchange_name": "Kucoin",
    "source": 32,
    "account_list": {
      "master-main": "master-main",
      "master-trade": "master-trade",
      "sub-main": "Sinodanish3-main",
      "sub-trade": "Sinodanish3-trade"
    },
    "default_account": { "xrp": "master-trade" },
    "lower_threshold": { "xrp": 300 },
    "upper_threshold": {
      "xrp": 300,
      "pax": 27500,
      "tusd": 27500,
      "btc": 5.5,
      "eos": 7000
    },
    "participating": 1,
    "main_account": {
      "account_type": "master-main",
      "account_name": "master-main"
    },
    "xrp": {
      "lower_threshold": 300,
      "upper_threshold": 300,
      "default_account": {
        "account_type": "master-trade",
        "account_name": "master-trade"
      }
    }
  }
}
    '''
    pathT = "./transfer_threshold.json"
    file_transfer = open(pathT, "r")
    tt = file_transfer.read()
    json_data = json.loads(tt)
    global transfer_thre_json
    transfer_thre_json = {}
    global trans_account
    trans_account = {}

    transfer_thre_json["balance_check_freq_ms"] = json_data["balance_check_freq_ms"]
    # transfer_thre_json["batch_vol"]=json_data["batch_vol"]
    for exchange in json_data["exchange"]:
        transfer_thre_json[exchange["source"]] = {}
        transfer_thre_json[exchange["source"]] = exchange
        transfer_thre_json[exchange["source"]]["main_account"] = {}
        transfer_thre_json[exchange["source"]]["main_account"]["account_type"] = exchange["account_list"][0][
            "account_type"]
        transfer_thre_json[exchange["source"]]["main_account"]["account_name"] = exchange["account_list"][0][
            "account_name"]
        for cur in exchange["lower_threshold"]:
            transfer_thre_json[exchange["source"]][cur] = {}
            transfer_thre_json[exchange["source"]][cur]["lower_threshold"] = exchange["lower_threshold"][cur]
            transfer_thre_json[exchange["source"]][cur]["standard_volume"] = exchange["standard_volume"][cur]
            transfer_thre_json[exchange["source"]][cur]["default_account"] = []
            deafult_account_name = exchange["default_account"][cur]
            trans_account[cur] = []
            account_temp = {}
            account_temp["account_name"] = exchange["default_account"][cur]
            account_temp["valid"] = False
            account_temp["source"] = exchange["source"]
            account_temp["trans_vol"] = 0
            account_temp["on_treading_vol"] = 0  # 在外部转账之后，对账户的余额进行虚增或者虚减。 等真正到账之后再清空
            account_temp["account_name"] = account_temp["account_type"] = ""
            for i in exchange["account_list"]:
                for acc in deafult_account_name:
                    if i["account_name"] == acc:
                        temp_acc = {}
                        temp_acc["account_name"] = acc
                        temp_acc["account_type"] = i["account_type"]
                        transfer_thre_json[exchange["source"]][cur]["default_account"].append(temp_acc)
                        account_temp["account_name"] = acc
                        account_temp["account_type"] = i["account_type"]
            trans_account[cur].append(account_temp)
        account_list = []
        account_list = exchange["account_list"]
        transfer_thre_json[exchange["source"]]["account_list"] = {}
        for account in account_list:
            transfer_thre_json[exchange["source"]]["account_list"][account["account_type"]] = account["account_name"]
    transfer_thre_json = unicode_convert(transfer_thre_json)

    global freq
    freq = transfer_thre_json["balance_check_freq_ms"] / 1000
    '''
    change Destination_address.json format
    {
        32: {
          "exchange": "Kucoin",
          "account_name": "sf_beavo",
          "source": 32,
          "WithdrawWhiteLists": {
            "btc": [ "375e6Hz5PEoC4AFZEq16a8naM9mKUDyDRS", "" ],
            "xrp": [ "rUW9toSjQkLY6EspdnBJP2paG4hWKmNbMh", "1862094677" ]
          }
        }
    }

    '''
    pathD = "./Destination_Address.json"
    file_destin = open(pathD, "r")
    da = file_destin.read()
    json_data = json.loads(da)
    file_destin.close()
    global destina_addr_json
    destina_addr_json = {}
    for dest in json_data["destination_address"]:
        destina_addr_json[dest["source"]] = dest
    destina_addr_json = unicode_convert(destina_addr_json)

    # global max_balance
    # max_balance = {}
    # for cur in transfer_thre_json["batch_vol"]:
    # 	max_balance[cur] = 0

    global transfer_balance_sr
    transfer_balance_sr = {}

    global transferred_value
    transferred_value = {}
    for source in transfer_thre_json:
        if type(source) == int:
            for cur in transfer_thre_json[source]["lower_threshold"]:
                addToDict2(transferred_value, source, cur, 0)

    # global batch_vol_amount
    # batch_vol_amount = {}
    # for bv in transfer_thre_json["batch_vol"]:
    # 	for key in transfer_thre_json:
    # 		if type(key)==int:
    # 			addToDict2(batch_vol_amount, bv,key, 0)

    for key in transfer_thre_json:
        if type(key) == int and transfer_thre_json[key]["participating"] == 1:
            context.add_td(source=key)

    context.insert_func_after_c(x=freq, y=reqpos)
    os.system("config=sell_it.conf wingchun strategy -p ./sell_it.py -n sell_it>sell.log")


def isFinishedQuery():
    global trans_account
    for cur in trans_account:
        for acc in trans_account[cur]:
            if acc["valid"] == False:
                return False
    return True


def clearQuery():
    global trans_account
    for cur in trans_account:
        for acc in trans_account[cur]:
            acc["valid"] = False


def getUpperAccount(cur, source):
    global trans_account, transfer_thre_json

    sorted(trans_account[cur], key=lambda x: x["transfer_vol"], reverse=True)
    for acc in trans_account[cur]:
        if acc["source"] == source and acc["transfer_vol"] > 0:
            return acc

    fee = transfer_thre_json[source]["withdrawal_minimum"][cur]

    for acc in trans_account:
        if acc["source"] == source or acc["transfer_vol"] > fee:
            continue
        return acc
    return None


def on_pos(context, pos_handler, request_id, source, rcv_time):
    if request_id == -1:
        context.pos_set = False
        return
    global transfer_thre_json, destina_addr_json, max_balance, transfer_balance_sr, transferred_value, mutex, trans_account
    if source in transfer_thre_json and transfer_thre_json[source]["participating"] == 1:  # source participat transfer

        # TODO : modify to find the one who is bigger than the standard
        for cur in transfer_thre_json[source]["lower_threshold"]:  # find which source cur value is biggest
            for account in trans_account[cur]:
                if pos_handler.get_account_name() == account["account_name"]:
                    print("196: " + str(source) + "," + cur + "_default_account_balance:" + account + " " + str(
                        pos_handler.get_long_tot(cur)))
                    if pos_handler.get_long_tot(cur) > transfer_thre_json[source][cur]["standard_volume"]:
                        print(str(pos_handler.get_long_tot(cur)))
                        print("account_name" + str(account["account_name"]))
                        trans_account[cur]["transfer_vol"] = pos_handler.get_long_tot(cur) - \
                                                             transfer_thre_json[source][cur][
                                                                 "standard_volume"] * 100000000 - trans_account[cur][
                                                                 "on_treading_vol"]
                        print("transfer_vol:" + str(trans_account[cur]["transfer_vol"]))
                        trans_account[cur]["valid"] = True
        if not isFinishedQuery():
            print("Not finished query")
            return

        # TODO modify to find the account who is lack of money
        for cur in transfer_thre_json[source]["lower_threshold"]:  # transfer general logic
            for account in transfer_thre_json[source][cur]["default_account"]:
                if pos_handler.get_account_name() == account["account_name"]:  # cur_max to lower main_account
                    print(cur + " upper standard account to lower account\n")

                    if pos_handler.get_long_tot(cur) < transfer_thre_json[source]["lower_threshold"][cur] * 100000000:
                        # after transfer tv volume,cur value in source < lower_threshold
                        print("after transfer tv volume,cur value in source < lower_threshold")
                        # while (pos_handler.get_long_tot(cur) + batch_vol_amount[cur][source] * transfer_thre_json["batch_vol"][cur]
                        # 	   * 100000000 < transfer_thre_json[source]["lower_threshold"][cur] * 100000000):
                        # 		batch_vol_amount[cur][source] += 1
                        # find compatible transfer volume
                        upperAccount = getUpperAccount(cur,source)
                        if upperAccount == None:
                            return

                        if account["source"] in transfer_thre_json:
                            if source == upperAccount["source"]:
                                # the transfer account and the aim account is in the same source
                                print("aim account is in the same source with upperAccount")
                                print("find aim account cur in source: " + str(source))
                                print("trasnfer_vol[cur]:" + str(upperAccount["transfer_vol"]))
                                inner_transfer(context=context,
                                                source = upperAccount["source"],
                                                cur = cur,
                                                volume = int(upperAccount["transfer_vol"]),
                                                from_type = upperAccount["account_type"],
                                                from_name = upperAccount["account_name"],
                                                to_type = account["account_type"],
                                                to_name = account["account_name"])
                            else:
                                print("aim account is not in the same source with upperAccount")
                                print("upper source " + str(upperAccount["source"]) + ",and aim source is " + source)
                                print("trasnfer_vol[cur]:" + str(upperAccount["transfer_vol"]) + "[{0}]".format(cur))
                                print("address:" + destina_addr_json[source]["WithdrawWhiteLists"][cur][0])
                                print("tag:" + destina_addr_json[source]["WithdrawWhiteLists"][cur][1])
                                withdraw(context=context,
                                         source = upperAccount["source"],
                                         currency = cur,
                                         volume = int(upperAccount["transfer_vol"]),
                                         from_type = upperAccount["account_type"],
                                         from_name = upperAccount["account_name"],
                                         to_source = source,
                                         to_type = account[ "account_type"],
                                         to_name=account["account_name"]
                                    )

        clearQuery()
        if not context.pos_set:
            context.data_wrapper.set_pos(pos_handler, source)

def on_transfer(context, pos_handler, request_id, source, rcv_time):
    context.log_info("on_transfer")
    print('on_transfer')

def on_error(context, error_id, error_msg, order_id, source, rcv_time):
    context.log_error('on_error:', error_id, error_msg)
    print('on_error:', error_id, error_msg)

def on_withdraw(context, withdraw, order_id, source, rcv_time):
    print
    "----on withdraw----"
    context.log_info("----on withdraw----")
    context.log_info("transfer_money:" + str(withdraw.Volume) + "rcv_time:" + str(rcv_time))
    print('----on withdraw----')

'''
source：交易所
cur: 币种
'''

def inner_transfer(context, source, cur, volume, from_type, from_name, to_type, to_name):

    print("[inner_transfer]")
    temp_name = to_name
    temp_type = to_type

    global transfer_thre_json
    trans_road = transfer_thre_json[source]["enable_transfer"]
    flag = False
    if from_name == to_name:  # the same account
        for aim in trans_road["self"]:
            if aim == to_type:
                flag = True
                print("[inner_transfer]find direct road int the same account")
                break
    else:  # not the same account
        for aim in trans_road["other"]:
            # find if there is direct road to transfer
            if (aim == to_type):
                flag = True
                print("[inner_transfer]find direct road int diff account")
                break
        if not flag:
            # if there isn't a direct orad, use recursion to find a appropriate road
            print("[inner_transfer] not find direct road ")
            for aim in trans_road["other"]:
                print("[inner_transfer]")
                for i in transfer_thre_json[source]["account_list"]:
                    # query a account_name
                    if i["account_type"] == aim:
                        temp_name = i["account_name"]
                        temp_type = aim
                flag = inner_transfer(context=context,
                                      source=source,
                                      cur=cur,
                                      volume=volume,
                                      from_type=temp_type,
                                      from_name=temp_name,
                                      to_type=to_type,
                                      to_name=to_name)
                if flag: break

    if not flag:
        print(" [inner_transfer]: can't find appropriate road to transfer")
        return False

    print (context.req_inner_transfer(
        source=source,
        currency=cur,
        volume=volume,
        from_type=from_type,
        from_name=from_name,
        to_type=temp_type,
        to_name=temp_name))

    print(" source: " + str(source) + " inner_transfer: " + cur + "\n")
    return True

'''
to_source: aim source
'''


def withdraw(context, source, cur, volume, from_type, from_name, to_source, to_type, to_name):
        print("[withdraw]")
        global transfer_thre_json, trans_account

        from_main_name = transfer_thre_json[source]["main_account"]["account_name"]
        from_main_type = transfer_thre_json[source]["main_account"]["account_type"]
        to_main_name = transfer_thre_json[to_source]["main_account"]["account_name"]
        to_main_type = transfer_thre_json[to_source]["main_account"]["account_type"]
        if (from_name != transfer_thre_json[source]["main_account"]["account_name"]):
            print("[withdraw] should transfer to main account")
            inner_transfer(context=context,
                           source=source,
                           cur=cur,
                           volume=volume,
                           from_type=from_name,
                           from_name=from_type,
                           to_type=from_main_type,
                           to_name=from_main_name)

        # 临时将当前账户余额虚增一个未来将会到账的转账金额（相当于假设外部转账提前到了），避免在等待外部转账到账过程中没有必要的再次触发，
        for cur in trans_account:
            for acc in trans_account[cur]:
                if acc["account_name"] == from_main_name and acc["account_type"] == from_main_type:
                    acc["on_treading_vol"] -= volume
                if acc["account_name"] == to_main_name and acc["account_type"] == to_main_type:
                    acc["on_treading_vol"] += volume

        print(context.withdraw_currency(source=source,
                                  currency=cur,
                                  volume=volume,
                                  address=destina_addr_json[source]["WithdrawWhiteLists"][cur][0],
                                  tag=destina_addr_json[source]["WithdrawWhiteLists"][cur][1]))

        # TODO: 查询历史记录
        # 外部转账完成，刚才提前虚增的金额就实际到账了，所以就不需要虚增的变量了
        for cur in trans_account:
            for acc in trans_account[cur]:
                if acc["account_name"] == from_main_name and acc["account_type"] == from_main_type:
                    acc["on_treading_vol"] += volume
                    if acc["account_name"] == to_main_name and acc["account_type"] == to_main_type:
                        acc["on_treading_vol"] -= volume

                    print("outer_transfer: " + cur + " from: " + str(source) + " to: " + str(to_source))
                    if (transfer_thre_json[to_source][cur]["default_account"]["account_name"]):
                        print("[withdraw] should transfer to sub account")
                    inner_transfer(context=context,
                                   source=source,
                                   cor=cur,
                                   volume= volume,
                                   from_name= to_main_name,
                                   from_type=to_main_type,
                                   to_type=to_type,
                                   to_name=to_name)
