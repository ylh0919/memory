#!/usr/bin/python
#import websocket
import thread
#import time
from SimpleWebSocketServer import SimpleWebSocketServer, WebSocket
from decimal import Decimal

global msg,currence,date,strike,kind
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
hand=[None]
selfsave = []
ticker_name = []
#ticker = ["btc_16aug19_11000_p"]
#pos = [None]
def webserver():
    class SimpleEcho(WebSocket):

        global msg
        #def handleMessage(self):
            # echo message back to client
            #selfsave.sendMessage(msg)

        def handleConnected(self):
            global msg
            if selfsave.count(self)==0:
                print msg
                self.sendMessage(msg)
                selfsave.append(self)
                print(self.address, 'connected')
                #print msg
                #self.sendMessage(msg)

        def handleClose(self):
            
            if selfsave.count(self)>0:
                selfsave.remove(self)
                print(self.address, 'closed')
            
    #server = SimpleWebSocketServer('', 8000, SimpleEcho)
    
    server = SimpleWebSocketServer('10.0.0.124', 8001, SimpleEcho)
    print 'good server'
    server.serveforever()

def on_ticker(context, tick, source, rcv_time):
    #print '1'
    global msg,currence,date,strike,kind
    #print '2'
    symbol=tick.InstrumentID
    #update_ticker(symbol)
    lowsymbol=symbol.lower()
    pos=hand[0].get_long_tot(lowsymbol)
    Ask_iv=Decimal(str(tick.Ask_iv)).quantize(Decimal('0.0'))
    Best_ask_amount=Decimal(str(tick.Best_ask_amount/1e8)).quantize(Decimal('0.0'))
    Best_ask_price=Decimal(str(tick.Best_ask_price/1e8)).quantize(Decimal('0.0000'))
    Best_bid_amount=Decimal(str(tick.Best_bid_amount/1e8)).quantize(Decimal('0.0'))
    Best_bid_price=Decimal(str(tick.Best_bid_price/1e8)).quantize(Decimal('0.0000'))
    Bid_iv=Decimal(str(tick.Bid_iv)).quantize(Decimal('0.0'))
    Mark_price=Decimal(str(tick.Mark_price/1e8)).quantize(Decimal('0.0000'))
    Last_price=Decimal(str(tick.Last_price/1e8)).quantize(Decimal('0.0000'))
    Open_interest=Decimal(str(tick.Open_interest)).quantize(Decimal('0.0'))
    Underlying_price=Decimal(str(tick.Underlying_price/1e8)).quantize(Decimal('0.00'))
    Delta=Decimal(str(tick.Delta)).quantize(Decimal('0.00'))
    Vega=Decimal(str(tick.Vega)).quantize(Decimal('0.00'))
    Volume24=Decimal(str(tick.Volume24/1e8)).quantize(Decimal('0.0'))
    Underlying_index=tick.Underlying_index
    fitted_IV=0

    currence=symbol[0:3]
    symbol=symbol[4: ]
    find=symbol.index('-')
    date=symbol[0:find]
    symbol=symbol[find+1: ]
    find=symbol.index('-')
    strike=symbol[0:find]
    kind=symbol[-1]

    #ticker = currence+'-'+date
    update_ticker(Underlying_index)

    end=symbol[-1]
    if end=='C':
        #currence=symbol[0:3]
        #symbol=symbol[4: ]
        #find=symbol.index('-')
        #date=symbol[0:find]
        #symbol=symbol[find+1: ]
        #find=symbol.index('-')
        #strike=symbol[0:find]
        #kind=symbol[-1]  
        msg0="{\"data\":[{\"Strike\":\""+strike+"\",\"put\":[\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\"],\"call\":[\""+str(pos)+"\",\""+str(Last_price)+"\",\""+str(Best_bid_amount)+"\",\""+str(Bid_iv)+"%"+"\",\""+str(Best_bid_price)+"\",\""+str(Mark_price)+"\",\""+str(Best_ask_price)+"\",\""+str(Ask_iv)+"%"+"\",\""+str(Best_ask_amount)+"\",\""+str(Volume24)+"\",\""+str(Open_interest)+"\",\""+str(Delta)+"\"]}],\"underlying_price\":"+str(Underlying_price)+",\"underlying_index\":\""+Underlying_index+"\",\"vega\":"+str(Vega)+",\"type\":\"Table\"}"
        
        msg1="{\"data\":[{\"Strike\":"+strike+",\"call_bid_IV\":"+str(Bid_iv)+",\"call_ask_IV\":"+str(Ask_iv)+",\"fitted_IV\":"+str(fitted_IV)+"}],\"underlying_price\":"+str(Underlying_price)+",\"underlying_index\":\""+Underlying_index+"\",\"type\":\"Chart\"}"
        for i in range(len(selfsave)):
            selfsave[i].sendMessage(msg0)
            selfsave[i].sendMessage(msg1)

    if end=='P':
        #currence=symbol[0:3]
        #symbol=symbol[4: ]
        #find=symbol.index('-')
        #date=symbol[0:find]
       # symbol=symbol[find+1: ]
       # find=symbol.index('-')
        #strike=symbol[0:find]
        #kind=symbol[-1]  
        msg0="{\"data\":[{\"Strike\":\""+strike+"\",\"put\":[\""+str(Last_price)+"\",\""+str(Best_bid_amount)+"\",\""+str(Bid_iv)+"%"+"\",\""+str(Best_bid_price)+"\",\""+str(Mark_price)+"\",\""+str(Best_ask_price)+"\",\""+str(Ask_iv)+"%"+"\",\""+str(Best_ask_amount)+"\",\""+str(Volume24)+"\",\""+str(Open_interest)+"\",\""+str(Delta)+"\",\""+str(pos)+"\"],\"call\":[\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\",\"-\"]}],\"underlying_price\":"+str(Underlying_price)+",\"underlying_index\":\""+Underlying_index+"\",\"vega\":"+str(Vega)+",\"type\":\"Table\"}"

        msg1="{\"data\":[{\"Strike\":"+strike+",\"put_bid_IV\":"+str(Bid_iv)+",\"put_ask_IV\":"+str(Ask_iv)+",\"fitted_IV\":"+str(fitted_IV)+"}],\"underlying_price\":"+str(Underlying_price)+",\"underlying_index\":\""+Underlying_index+"\",\"type\":\"Chart\"}"
        for i in range(len(selfsave)):
            selfsave[i].sendMessage(msg0)
            selfsave[i].sendMessage(msg1)

def on_pos(context, pos_handler, request_id, source, rcv_time):
    if request_id == -1:
        if pos_handler is None:
            print '-- got no pos in initial, so req pos --'
            context.req_pos(source=SOURCE.DERIBIT)
            context.pos_set = False
            return
        else:
            print '-- got pos in initial --'
            hand[0]=pos_handler
            context.print_pos(pos_handler)
            '''
            for i in range (1):
                pos[i] = pos_handler.get_long_tot(ticker[i])
                print pos[i]
            '''
            #context.stop()
    else:
        print '-- got pos requested --'
        context.print_pos(pos_handler)
        if not context.pos_set:
            context.data_wrapper.set_pos(pos_handler, source)
        #context.stop()

def update_ticker(name):
    global msg
    #print 'update'
    if ticker_name.count(name)==0:
        ticker_name.append(name)
    msg = "{\"data\":["
    for i in range(len(ticker_name)-1):
        msg = msg + "\"" + ticker_name[i] + "\","
    msg = msg + "\"" + ticker_name[len(ticker_name)-1] + "\"],\"type\":\"Chart_index\"}"
    #print msg
