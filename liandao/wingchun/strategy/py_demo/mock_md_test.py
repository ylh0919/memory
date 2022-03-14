def initialize(context):
    context.add_md(source=SOURCE.MOCK)
    context.exchange_id = EXCHANGE.SHFE
    context.subscribe(tickers=['btc_usdt_aug','btc_usdt'], source=SOURCE.MOCK)
count =1
start = 1
last_time = 0
delta_min = 1000000000
f = open("./mock_data.txt","w")
def on_price_book(context, price_book, source, rcv_time):
    global count
    global start
    global last_time
    global delta_min
    if(count == 1): start = rcv_time
    delta = rcv_time - last_time
    f.write(str(delta)+"\n")
    if(delta<delta_min): delta_min = delta
    print 'start:'+ str(start)
    print 'min delta = ' + str(delta_min) + "\n"
    print 'the time delta:' + str(delta)
    print 'symbol:'+ price_book.InstrumentID+',time:'+str(rcv_time)+',count:' +str(count)
    count+=1
    last_time = rcv_time
