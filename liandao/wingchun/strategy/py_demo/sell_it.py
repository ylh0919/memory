import functools, random, sys

global_order_id_ticker_map = {}

maker_order_retry_seconds = 1;
maker_order_fail_retry_seconds = 1;


class top_of_book:

    def __init__(self, top_bid, top_ask):
        self.top_bid = top_bid
        self.top_ask = top_ask

    def is_valid(self):
        return self.top_bid != None and self.top_ask != None and self.top_ask >= self.top_bid

def print_log(context, text):
    print text
    context.log_info(text)

def clear_order_status(order_status):
    order_status[0]=-1
    order_status[1] = 0
    order_status[2] = 0
    order_status[3] = None
    order_status[4] = None

def clear_maker_status(maker_status):
    maker_status[0] = 0
    maker_status[1] = None

def get_source_from_exchange(exch_name):
    if exch_name == "oceanex":
        return SOURCE.OCEANEX
    elif exch_name == "bitmex":
        return SOURCE.BITMEX
    else:
        return None

def scale_volume(src_volume):
    return src_volume * 1e8

def add_timer(context, interval, func, msg):
    print_log(context, "add_timer, {}, {}".format(interval, msg))
    context.insert_func_after_c(interval, func)

def timer_callback(symbol, context):
    if symbol in context.white_list and symbol in context.order_status:
        print_log(context, "timer_callback")
        if context.white_list[symbol][4] == 0:
            check_and_send_taker_order(symbol, context)
        else:
            check_and_send_maker_order(symbol, context, True)

def check_and_send_taker_order(symbol, context):
    order_status = context.order_status[symbol]
    config = context.white_list[symbol]
    if order_status[0] == -1 and config[2] > 0:
        target_qty = config[2] if config[2] < config[1] else config[1]
        
        log_text = "send a {} market order, {}, {}, {}".format("buy" if config[3]==DIRECTION.Buy else "sell", symbol, target_qty, config[2]) 
        print_log(context, "going to {}".format(log_text))
        
        order_status[1] = target_qty
        order_status[2] = 0
        order_status[3] = config[3]
        order_status[0] = context.insert_market_order(source=context.exch_src,
                                                ticker=symbol,
                                                exchange_id=context.exch_id,
                                                volume=order_status[1],
                                                direction=config[3],
                                                offset=OFFSET.Open)
            
        if order_status[0] == -1:
            print_log(context, "failed to {}".format(log_text))
            clear_order_status(order_status)
        else:
            global_order_id_ticker_map[order_status[0]] = symbol
    else:
        print_log(context, "no action for check_and_send_taker_order")


    if config[2] > 0:
        add_timer(context, context.white_list[symbol][0], functools.partial(timer_callback, symbol), "check_and_send_taker_order: next time to begin a new taker iteration")
    else:
        context.complete_list[symbol] = {}
        if len(context.complete_list) == len(context.white_list):
            print_log(context, "we have done all tickers, exit...")
            sys.exit(0)    
        
def check_and_send_maker_order(symbol, context, from_timer_cb):
    config = context.white_list[symbol]
    order_status = context.order_status[symbol]
    maker_status = context.maker_status[symbol]
    tob = context.top_of_book[symbol]
    
    if maker_status[0] == 0:
        # set to in_maker and set timer to transit to maker and send limit order
        if order_status[0] == -1 and config[2] > 0 and tob.is_valid():
            target_qty = config[2] if config[2] < config[1] else config[1]
               
            tob_spread = tob.top_ask - tob.top_bid
            price_offset = int(tob_spread * config[6])

            target_price = tob.top_bid + price_offset if config[3] == DIRECTION.Buy else tob.top_ask - price_offset

            log_text = "send a {} limit order, {}, {}, {}/{} {}, {}".format("buy" if config[3]==DIRECTION.Buy else "sell", symbol, target_qty, target_price, tob.top_bid, tob.top_ask, config[2]) 
            print_log(context, "going to {}".format(log_text))
        
            order_status[1] = target_qty
            order_status[2] = 0
            order_status[3] = config[3]
            order_status[4] = target_price

            order_status[0] = context.insert_limit_order(source=context.exch_src,
                                                   ticker=symbol,
                                                   price=target_price,
                                                   exchange_id=context.exch_id,
                                                   volume=target_qty,
                                                   direction=config[3],
                                                   offset=OFFSET.Open)
            
            if order_status[0] == -1:
                print_log(context, "failed to {}".format(log_text))
                clear_order_status(order_status)
                sys.exit(1)
            else:
                global_order_id_ticker_map[order_status[0]] = symbol
                maker_status[0] = 1
                maker_status[1] = target_qty
                maker_status[2] = False
                add_timer(context, config[5], functools.partial(timer_callback, symbol), "check_and_send_maker_order: next time to switch from maker to taker")
        else:
            if config[2] > 0:
                add_timer(context, maker_order_retry_seconds, functools.partial(timer_callback, symbol), "check_and_send_maker_order: next time to retry another maker")
            else:
                context.complete_list[symbol] = {}
                if len(context.complete_list) == len(context.white_list):
                    print_log(context, "we have done all tickers, exit...")
                    sys.exit(0)    

    elif maker_status[0] == 1:
        if order_status[0] != -1 and order_status[1] > 0:
            log_text = "cancel maker order {}".format(order_status[0])
            print_log(context, "going to {}".format(log_text))
            order_status[0] = context.cancel_order(source=context.exch_src, order_id=order_status[0])
            if order_status[0] == -1:
                print_log(context, "failed to {}".format(log_text))
                sys.exit(1)
            else:
                order_status[1] = 0
                global_order_id_ticker_map[order_status[0]] = symbol
                if from_timer_cb:
                    maker_status[0] = 2
                    add_timer(context, (config[0] - config[5]), functools.partial(timer_callback, symbol), "check_and_send_maker_order: next time to begin a new maker iteration")
        elif order_status[0] != -1 and order_status[1] == 0:
            if from_timer_cb:
                maker_status[0] = 2
                add_timer(context, (config[0] - config[5]), functools.partial(timer_callback, symbol), "check_and_send_maker_order: next time to begin a new maker iteration")
        elif order_status[0] == -1:
            if from_timer_cb:
                maker_status[0] = 2
                add_timer(context, (config[0] - config[5]), functools.partial(timer_callback, symbol), "check_and_send_maker_order: next time to begin a new maker iteration")
                
                if maker_status[1] > 0:
                    check_and_send_maker_order(symbol, context, False)
            elif maker_status[1] > 0:
                target_qty = maker_status[1]
               
                tob_spread = tob.top_ask - tob.top_bid
                price_offset = int(tob_spread * config[6])

                target_price = tob.top_bid + price_offset if config[3] == DIRECTION.Buy else tob.top_ask - price_offset

                log_text = "send a {} limit order, {}, {}, {}/{} {}, {}".format("buy" if config[3]==DIRECTION.Buy else "sell", symbol, target_qty, target_price, tob.top_bid, tob.top_ask, config[2]) 
                print_log(context, "going to {}".format(log_text))
        
                order_status[1] = target_qty
                order_status[2] = 0
                order_status[3] = config[3]
                order_status[4] = target_price

                order_status[0] = context.insert_limit_order(source=context.exch_src,
                                                                ticker=symbol,
                                                                price=target_price,
                                                                exchange_id=context.exch_id,
                                                                volume=target_qty,
                                                                direction=config[3],
                                                                offset=OFFSET.Open)
            
                if order_status[0] == -1:
                    print_log(context, "failed to {}".format(log_text))
                    clear_order_status(order_status)
                    sys.exit(1)
                else:
                    global_order_id_ticker_map[order_status[0]] = symbol
    elif maker_status[0] == 2:
        if from_timer_cb:
            maker_status[2] = True

        if order_status[0] == -1:
            if maker_status[1] == 0:
                if maker_status[2]:
                    maker_status[0] = 0
                    maker_status[2] = False
                    check_and_send_maker_order(symbol, context, True)
            else:
                log_text = "send a {} market order, {}, {}, {}".format("buy" if config[3]==DIRECTION.Buy else "sell", symbol, maker_status[1], config[2]) 
                print_log(context, "going to {}".format(log_text))
        
                order_status[1] = maker_status[1]
                order_status[2] = 0
                order_status[3] = config[3]
                order_status[0] = context.insert_market_order(source=context.exch_src,
                                                              ticker=symbol,
                                                              exchange_id=context.exch_id,
                                                              volume=order_status[1],
                                                              direction=config[3],
                                                              offset=OFFSET.Open)
            
                if order_status[0] == -1:
                    print_log(context, "failed to {}".format(log_text))
                    clear_order_status(order_status)
                    sys.exit(1)
                else:
                    global_order_id_ticker_map[order_status[0]] = symbol

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

    context.add_md(source=context.exch_src)
    context.add_td(source=context.exch_src)
    context.subscribe(tickers=map(lambda x : x[0], context.white_list.items()), source=context.exch_src)

    print context.white_list
    
def on_price_book(context, price_book, source, rcv_time):
    if price_book.InstrumentID in context.white_list:
        config = context.white_list[price_book.InstrumentID]
        if config[4] == 1:
            tob = context.top_of_book[price_book.InstrumentID]
            if price_book.BidLevelCount == 0 or price_book.AskLevelCount == 0 or price_book.AskLevels.levels[0].price <= price_book.BidLevels.levels[0].price:
                tob.top_bid = None
                tob.top_ask = None
            else:
                tob.top_bid = price_book.BidLevels.levels[0].price
                tob.top_ask = price_book.AskLevels.levels[0].price
    
            order_status = context.order_status[price_book.InstrumentID]
            if order_status[0] != -1 and order_status[1] > 0:
                need_requote = False;
                if config[3] == DIRECTION.Buy and tob.top_bid > order_status[4]:
                    need_requote = True

                if config[3] == DIRECTION.Sell and tob.top_ask < order_status[4]:
                    need_requote = True

                if need_requote:
                    check_and_send_maker_order(price_book.InstrumentID, context, False)


def on_pos(context, pos_handler, request_id, source, rcv_time):
    print("on_pos,", pos_handler, request_id, source, rcv_time)
    if request_id == -1:
        if pos_handler is None:
            print '-- got no pos in initial, so req pos --'
            context.req_pos(context.exch_src)
            context.pos_set = False
            return
        else:
            print '-- got pos in initial --'
            context.print_pos(pos_handler)
            context.data_wrapper.set_pos(pos_handler, source)
            context.pos_set = True
    else:
        print '-- got pos requested --'
        context.print_pos(pos_handler)
        print(pos_handler)
        if not context.pos_set:
            context.data_wrapper.set_pos(pos_handler, source)
            context.pos_set = True
    
    for t in context.white_list.keys():
        if context.white_list[t][4] == 0:
            check_and_send_taker_order(t, context)
        else:
            check_and_send_maker_order(t, context, True)

def on_rtn_order(context, rtn_order, order_id, source, rcv_time):
    symbol = rtn_order.InstrumentID
    log_text = "on_rtn_order {}, {}, {}, {}".format(order_id, symbol, rtn_order.OrderStatus, rtn_order.VolumeTraded)
    print_log(context, log_text)

    if symbol in context.white_list and order_id in global_order_id_ticker_map:
        if rtn_order.OrderStatus == 'b':
            pass
        elif rtn_order.OrderStatus in ('0', '5', 'd'):
            order_status = context.order_status[symbol]
            clear_order_status(order_status)
            
            config = context.white_list[symbol]

            if config[2] >= rtn_order.VolumeTraded:
                config[2] = config[2] - rtn_order.VolumeTraded
            else:
                config[2] = 0
            
            if config[4] == 1:
                maker_status = context.maker_status[symbol]
                if maker_status[1] >= rtn_order.VolumeTraded:
                    maker_status[1] = maker_status[1] - rtn_order.VolumeTraded
                else:
                    maker_status[1] = 0
                
                check_and_send_maker_order(symbol, context, False)
    else:
        print_log(context, "on_rtn_order, unknown order {}, {}".format(symbol, order_id))

def on_error(context, error_id, error_msg, order_id, source, rcv_time):
    symbol = "unknown"
    if order_id in global_order_id_ticker_map:
        symbol = global_order_id_ticker_map[order_id]
    log_text = "on_error {}, {}, {}, {}".format(symbol, error_id, error_msg, order_id)
    print_log(context, log_text)
    
    config = context.white_list[symbol]

    order_status = context.order_status[symbol]
    if order_id == order_status[0]:
        clear_order_status(order_status)
        if config[4] == 1:
            check_and_send_maker_order(symbol, context, False)
    else:
        print_log(context, "on_error, unknown order id {}".format(order_id))

def on_rtn_trade(context, rtn_trade, order_id, source, rcv_time):
    symbol = rtn_trade.InstrumentID
    log_text = "on rtn trade {}, {}, {}, {}".format(symbol, order_id, rtn_trade.Price, rtn_trade.Volume)
    print_log(context, log_text)
    
"""
    if symbol in context.white_list and order_id in global_order_id_ticker_map:
        config = context.white_list[symbol]
        if config[2] >= rtn_trade.Volume:
            config[2] = config[2] - rtn_trade.Volume
        else:
            config[2] = 0
    else:
        print_log(context, "on_trn_trade, unknown trade {}, {}".format(symbol, order_id))
"""
