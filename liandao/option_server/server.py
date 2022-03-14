#!/usr/bin/python
#import websocket
import thread
#import time
from SimpleWebSocketServer import SimpleWebSocketServer, WebSocket
from decimal import Decimal

def initialize(context):
    context.add_md(source=SOURCE.DERIBIT)
    context.ticker = 'btc_usdt'
    context.exchange_id = EXCHANGE.SHFE
    context.buy_price = -1
    context.sell_price = -1
    context.order_rid = -1
    context.cancel_id = -1
    context.pos_set = True
    context.add_td(source=SOURCE.DERIBIT)
    context.subscribe(tickers=[context.ticker], source=SOURCE.DERIBIT)
    thread.start_new_thread( webserver, ( ) )

#global selfsave
#selfsave=None
selfsave = [None,None,None,None,None]
def webserver():
    class SimpleEcho(WebSocket):

        global msg
        #def handleMessage(self):
            # echo message back to client
            #selfsave.sendMessage(msg)

        def handleConnected(self):
            #global selfsave
            for i in range(5):
                if selfsave[i] == None:
                    selfsave[i]=self
                    print(self.address, 'connected')
                    break            

        def handleClose(self):
            #global selfsave
            for i in range(5):
                if selfsave[i] == self:
                    selfsave[i]=None
                    print(self.address, 'closed')
                    break

    #server = SimpleWebSocketServer('', 8000, SimpleEcho)
    server = SimpleWebSocketServer('172.31.41.167', 8001, SimpleEcho)
    print 'good server'
    server.serveforever()

#global selfsave
def on_ticker(context, tick, source, rcv_time):
    global msg
    #global selfsave
    symbol=tick.InstrumentID
    Ask_iv=Decimal(str(tick.Ask_iv/100)).quantize(Decimal('0.0'))
    Best_ask_amount=Decimal(str(tick.Best_ask_amount/1e8)).quantize(Decimal('0.0'))
    Best_ask_price=Decimal(str(tick.Best_ask_price/1e8)).quantize(Decimal('0.0000'))
    Best_bid_amount=Decimal(str(tick.Best_bid_amount/1e8)).quantize(Decimal('0.0'))
    Best_bid_price=Decimal(str(tick.Best_bid_price/1e8)).quantize(Decimal('0.0000'))
    Bid_iv=Decimal(str(tick.Bid_iv/100)).quantize(Decimal('0.0'))
    Mark_price=Decimal(str(tick.Mark_price/1e8)).quantize(Decimal('0.0000'))
    Last_price=Decimal(str(tick.Last_price/1e8)).quantize(Decimal('0.0000'))
    Open_interest=Decimal(str(tick.Open_interest)).quantize(Decimal('0.0'))
    Underlying_price=Decimal(str(tick.Underlying_price/1e8)).quantize(Decimal('0.00'))
    Delta=Decimal(str(tick.Delta)).quantize(Decimal('0.00'))
    Vega=Decimal(str(tick.Vega)).quantize(Decimal('0.00'))
    Volume24=Decimal(str(tick.Volume24/1e8)).quantize(Decimal('0.0'))
    Underlying_index=tick.Underlying_index

    end=symbol[-1]
    if end=='C':
        currence=symbol[0:3]
        symbol=symbol[4: ]
        find=symbol.index('-')
        date=symbol[0:find]
        symbol=symbol[find+1: ]
        find=symbol.index('-')
        strike=symbol[0:find]
        kind=symbol[-1]  
        msg="{\"data\":[{\"Strike\":\""+strike+"\",\"put\":[\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\"],\"call\":[\""+str(Volume24)+"\",\""+str(Last_price)+"\",\""+str(Best_bid_amount)+"\",\""+str(Bid_iv)+"%"+"\",\""+str(Best_bid_price)+"\",\""+str(Mark_price)+"\",\""+str(Best_ask_price)+"\",\""+str(Ask_iv)+"%"+"\",\""+str(Best_ask_amount)+"\",\""+str(Volume24)+"\",\""+str(Open_interest)+"\",\""+str(Delta)+"\"]}],\"expity_date\":\""+date+"\",\"coin\":\""+currence+"\",\"underlying_price\":\""+str(Underlying_price)+"\",\"underlying_index\":\""+Underlying_index+"\",\"vega\":\""+str(Vega)+"\"}"
        for i in range(5):
            if selfsave[i] != None :
                selfsave[i].sendMessage(msg)

    if end=='P':
        currence=symbol[0:3]
        symbol=symbol[4: ]
        find=symbol.index('-')
        date=symbol[0:find]
        symbol=symbol[find+1: ]
        find=symbol.index('-')
        strike=symbol[0:find]
        kind=symbol[-1]  
        msg="{\"data\":[{\"Strike\":\""+strike+"\",\"put\":[\""+str(Last_price)+"\",\""+str(Best_bid_amount)+"\",\""+str(Bid_iv)+"%"+"\",\""+str(Best_bid_price)+"\",\""+str(Mark_price)+"\",\""+str(Best_ask_price)+"\",\""+str(Ask_iv)+"%"+"\",\""+str(Best_ask_amount)+"\",\""+str(Volume24)+"\",\""+str(Open_interest)+"\",\""+str(Delta)+"\",\""+str(Volume24)+"\"],\"call\":[\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\"]}],\"expity_date\":\""+date+"\",\"coin\":\""+currence+"\",\"underlying_price\":\""+str(Underlying_price)+"\",\"underlying_index\":\""+Underlying_index+"\",\"vega\":\""+str(Vega)+"\"}"
        for i in range(5):
            if selfsave[i] != None :
                selfsave[i].sendMessage(msg)

'''
def on_pos(context, pos_handler, request_id, source, rcv_time):
    if request_id == -1:
        if pos_handler is None:
            print '-- got no pos in initial, so req pos --'
            context.req_pos(source=SOURCE.OCEANEX)
            context.pos_set = False
            return
        else:
            print '-- got pos in initial --'
            context.print_pos(pos_handler)
            context.stop()
    else:
        print '-- got pos requested --'
        context.print_pos(pos_handler)
        if not context.pos_set:
            context.data_wrapper.set_pos(pos_handler, source)
        context.stop()
'''
