def initialize(context):
    context.add_md(source=SOURCE.BINANCE)
    context.exchange_id = EXCHANGE.SHFE
    context.subscribe(tickers=['etc_eth'], source=SOURCE.BINANCE)
count =1
start = 1
last_time = 0
f = open("./binance_105_data.txt","w")
def on_l2_trade(context, trade_data, source, rcv_time):
    global count
    global start
    global last_time
    global f
    if(count == 1): start = rcv_time
    delta = rcv_time - last_time
    f.write(str(rcv_time) + " ")
    f.write(str(delta)+"\n")
    print 'start:'+ str(start)
    print 'the time delta:' + str(delta)
    print 'symbol:'+ trade_data.InstrumentID+',time:'+str(rcv_time)+',count:' +str(count)
    count+=1
    last_time = rcv_time
