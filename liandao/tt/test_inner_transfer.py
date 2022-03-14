

def initialize(context):
    context.add_td(source=32)


def on_pos(context, pos_handler, request_id, source, rcv_time):
    if request_id == -1:
        context.pos_set = False
        context.req_pos(source=32,
                        account_type="master-main",
                        account_name="")
    else:
	print pos_handler.get_long_tot("xrp")
        print context.req_inner_transfer(source=32,
                                    currency="xrp",
                                    volume=int(0 * 100000000),
                                    from_type="master-trade",
                                    from_name="",
                                    to_type="master-main",
                                    to_name="")
        if not context.pos_set:
            context.data_wrapper.set_pos(pos_handler, source)


def on_transfer(context, transfer, request_id, source, rcv_time):
    print('on_transfer')
    context.log_info("on_transfer")


def on_error(context, error_id, error_msg, order_id, source, rcv_time):
    context.log_error('on_error:', error_id, error_msg)
    print('on_error:', error_id, error_msg)


def on_withdraw(context, withdraw, order_id, source, rcv_time):
    print('----on withdraw----')
    context.log_info("----on withdraw----")
    context.log_info("transfer_money:" + str(withdraw.Volume) + "rcv_time:" + str(rcv_time))
