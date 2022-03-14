'''
Copyright [2017] [taurus.ai]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

'''
test limit order and order cancelling for new wingchun strategy system.
you may run this program by:
wingchun strategy -n my_test -p /liandao/wingchun/strategy/py_demo/binance_order_cancel_test_td_only.py
'''

def initialize(context):
    #context.add_md(source=SOURCE.COINMEX)
    context.ticker = 'btc_6000_put'
    context.exchange_id = EXCHANGE.SHFE
    context.buy_price = -1
    context.sell_price = -1
    context.order_rid = -1
    context.quote_id = -1
    context.cancel_id = -1
    context.add_md(source=SOURCE.FTX)
    context.subscribe(tickers=[context.ticker], source=SOURCE.FTX)
    context.add_td(source=SOURCE.FTX)
def on_pos(context, pos_handler, request_id, source, rcv_time):
    print("on_pos,", pos_handler, request_id, source, rcv_time)
    if request_id == -1:
        if pos_handler is None:
            print '-- got no pos in initial, so req pos --'
            context.req_pos(source=SOURCE.FTX)
            context.pos_set = False
            return
        else:
            print '-- got pos in initial --'
            context.print_pos(pos_handler)
            #context.stop()
            print '----will test sell cancel----'
            context.buy_price = 800000000000 #market_data.LowerLimitPrice
            context.sell_price = 2000000000000 #market_data.UpperLimitPrice
            if context.order_rid < 0:
                print("context.insert_limit_order 1.")
                context.order_rid = context.insert_quote_request(source=SOURCE.FTX,
                                                               ticker=context.ticker,
                                                               expiry = '2020-08-28T03:00:00Z',
                                                               exchange_id=context.exchange_id,
                                                               volume=10000,
                                                               direction=DIRECTION.Buy)
                print("context.order_rid:", context.order_rid)
                print('will cancel it')
            if context.quote_id > 0:
                print("context.insert_limit_order 1.")
                context.quote_id = context.insert_quote(source,context.ticker,157516866,100000000)
                print("context.quote_id:", context.quote_id)
                print('will cancel it')
    else:
        print '-- got pos requested --'
        context.print_pos(pos_handler)
        print(pos_handler)
        if not context.pos_set:
            context.data_wrapper.set_pos(pos_handler, source)


def on_quote_requests(context,quote_request_data,source,rcv_time):
    print('on_rtn_quote instument:', quote_request_data.InstrumentID,"id:",quote_request_data.ID)
    #context.quote_id = context.insert_quote(source,context.ticker,quote_request_data.ID,100000000)

def on_rtn_quote(context, rtn_quote, req_id,source, rcv_time):
    print('on_rtn_quote', rtn_quote.ID)
    if req_id == context.quote_id and context.cancel_id < 0 and rtn_quote.OrderStatus != 'a':
        context.cancel_id = context.cancel_quote(source=source, quote_id=req_id)
        print 'cancel (quote id)', rtn_quote.ID, ' (request_id)', context.cancel_id
    elif req_id == context.order_rid:
        tmpid = input('quote id')
        context.accept_id = context.accept_quote(source=source, quote_id=tmpid)
        print 'cancel (quote id)', rtn_quote.ID, ' (request_id)', context.accept_id
    if req_id == context.quote_id and rtn_quote.OrderStatus == '5':
        print 'cancel quote successfully!'
        context.stop()
def on_rtn_order(context, rtn_order, order_id, source, rcv_time):
    print('on_rtn_quote ', rtn_order.OrderStatus)
    if order_id == context.order_rid and context.cancel_id < 0 and rtn_order.OrderStatus != 'a':
        #context.cancel_id = context.cancel_quote_request(source=source, quote_request_id=order_id)
        print 'cancel (order_id)', order_id, ' (remote_id)', rtn_order.BusinessUnit
        tmpid = input('quote id')
        context.accept_id = context.accept_quote(source=source, quote_id=tmpid)
        print 'cancel (quote id)', tmpid, ' (request_id)', context.accept_id
    if order_id == context.order_rid and rtn_order.OrderStatus == '5':
        print 'cancel successfully!'
        context.stop()

def on_error(context, error_id, error_msg, order_id, source, rcv_time):
    print 'on_error:', error_id, error_msg, order_id, source, rcv_time

def on_rtn_trade(context, rtn_trade, order_id, source, rcv_time):
    print '----on rtn trade----'
