from CoreObjects.ClientQuote import create_client_quote
from CoreObjects.Deal import Deal
from CoreObjects.Order import Order
from CoreObjects.OrderBook import OrderBook

__author__ = 'ruayhg'

import pandas as pd
import numpy as np
from datetime import datetime
import re



def client_parser(client_logs_path, year, month, day):
    path_for_IOC_orders = "C:\\Users\\ruayhg\\PycharmProjects\\BigLogs\\local_orders.log"
    local_orders = pd.read_csv(path_for_IOC_orders, sep=":;", header=None)
    local_orders['Order'], local_orders['isIOCOrder'] = np.vectorize(parse_initial_order)(local_orders[0], year, month, day)
    order_indices = local_orders[local_orders['isIOCOrder'] == True].index
    for starting_index in order_indices:
        order = local_orders.loc[starting_index, 'Order']
        deal = Deal(order)
        deal_array = local_orders.loc[starting_index:(starting_index+200), 0]
        for j, row in deal_array.iteritems():
            status = fill_client_deal_part(row, deal)
            if status == 'End of Deal':
                break
    path_for_Limit_orders = "C:\\Users\\ruayhg\\PycharmProjects\\BigLogs\\limit_orders.csv"
    limit_orders =  pd.read_csv(path_for_Limit_orders, sep=":;", header=None)


def parse_validation_price(deal, row):
    fill_method_execution_time(deal, row, "Validate Price")
    deal.validation_price = float(re.search(' price = (.+?)', row).group(1))

'00| 09:53:58.249 21601374.672032 ->   LocalOrders: ---- FXBA::V2::ORDER::ENTRY: id = 10100160, action = 1, ' \
'instr_name = USD/RUB_TOM, aggr_id = 10100160, external_id = , ifoco_params = , src_name = RZBM, trader = psbf.api,' \
' side = 1, lot = 500000.000000, executed_lot = 0.000000, leaves_lot = 500000.000000, price = 54.837000, ' \
'set_time = 130797752372358140, change_time = 130797752372358140, status = 2, user_order_id = F2-153583, ' \
'aggr_src_name = RZBM, avg_exec_price = 0.000000, show_lot = 1000000000000.000000, exec_type = 5, tif = 1, ' \
'tif_time = 0, flags = 0, comment_text = |FIX|, ver = 0, uid = 0, client_id = 109, min_lot = 0.000000'
def parse_end_of_deal(deal, row):
    fill_method_execution_time(deal, row, "End of Deal")
    executed_lot = float(re.search(' executed_lot = (.+?)', row).group(1))
    executed_price = float(re.search(' avg_exec_price = (.+?)', row).group(1))
    deal.executed_lot, deal.executed_price = executed_lot, executed_price


def fill_client_deal_part(row, deal):
    if ('selected group' in row) and (deal.hedging_group is None):
        parse_hedging_group(deal, row)
        return None
    elif ('order_id: ' in row) and (deal.order_id is None):
        parse_order_id(deal, row)
        return None
    elif ('LocalOrders: SendOrderToSubscribers' in row) and (deal.aggr_id is None):
        parse_aggr_id_set_change_time(deal,row)
        return None
    elif 'exist = ' in row:
        parse_best_side_quote(deal, row)
        return None
    elif 'price_tolerance =' in row:
        parse_tolerance(deal, row)
        return None
    elif ('exist = ' not in row) and ('FXBA::V2::BOOK2::ENTRY' in row):
        parse_book_quote(deal, row)
        return None
    elif ('process no better prices' in row):
        parse_no_better_prices(deal,row)
        return None
    elif ('LocalOrders: src deal: quote time (ms)' in row):
        parse_quote_time(deal, row)
        return None
    elif ('LocalOrders: src deal: LocalOrdersTypes::DEAL::REQ: FXBA::V2::DEAL::REQ:' in row):
        parse_validation_price(deal, row)
        return None
    elif ('LocalOrders: ---- FXBA::V2::ORDER::ENTRY:' in row):
        parse_end_of_deal(deal,row)
        return 'End of Deal'

'00| 08:14:14.374 21595390.966858 ->   bid  : exist = 1, FXBA::V2::BOOK2::ENTRY: id = 0, action = 8, type = 0, ' \
'price = 1.118420, volume = 500000.000000, flags = 1, ord_data = vector, 0 entries'
def parse_best_side_quote(deal, row):
    fill_method_execution_time(deal, row, "Best Side")
    id = int(re.search('id = (.+?),', row).group(1))
    side = int(re.search('type = (.+?),', row).group(1))
    price = float(re.search(' price = (.+?),', row).group(1))
    instrument = deal.order.ccy_pair
    book_type = int(re.search('flags = (.+?),', row).group(1))
    size = float(re.search('volume = (.+?),', row).group(1))
    client_quote = create_client_quote(id, side, price, instrument, book_type, size)
    deal.client_book = OrderBook(book_type, "ClientBook")
    if client_quote.Side == 'Bid':
        deal.best_bid = client_quote
    elif client_quote.Side == 'Ask':
        deal.best_ask = client_quote
    else:
        pass

'00| 08:14:14.390 21595390.966991 ->   FXBA::V2::BOOK2::ENTRY: id = 0, action = 8, type = 0, price = 1.118420, ' \
'volume = 500000.000000, flags = 1, ord_data = vector, 0 entries'
def parse_book_quote(deal, row):
    fill_method_execution_time(deal, row, "Get Quote")
    id = int(re.search('id = (.+?),', row).group(1))
    side = int(re.search('type = (.+?),', row).group(1))
    price = float(re.search(' price = (.+?),', row).group(1))
    instrument = deal.order.ccy_pair
    book_type = int(re.search('flags = (.+?),', row).group(1))
    size = float(re.search('volume = (.+?),', row).group(1))
    client_quote = create_client_quote(id, side, price, instrument, book_type, size)
    deal.client_book.add_quote(client_quote)

'00| 08:14:14.374 21595390.966874 ->   price_tolerance = 0.000005'
def parse_tolerance(deal, row):
    fill_method_execution_time(deal, row, "Get Tolerance")
    deal.price_tolerance = float(re.search('price_tolerance = (.+?)', row).group(1))

'00| 08:14:14.390 21595390.967246 ->   	process no better prices'
def parse_no_better_prices(deal, row):
    fill_method_execution_time(deal, row, "No better prices")
    deal.no_better_prices = True

'00| 09:53:57.235 21601373.667698 ->   LocalOrders: src deal: quote time (ms): 1981'
def parse_quote_time(deal, row):
    fill_method_execution_time(deal, row, "Quote Lifetime")
    deal.quote_lifetime = float(re.search('quote time (ms): (.+?)', row).group(1))


'00| 08:14:14.374 21595390.966659 ->   LocalOrders: SendOrderToSubscribers, action: 1, FXBA::V2::ORDER::ENTRY:' \
' id = 10099850, action = 1, instr_name = EUR/USD, aggr_id = 10099850, external_id = , ifoco_params = , src_name = RZBM, ' \
'trader = acbk.bbg, side = 0, lot = 50000.000000, executed_lot = 0.000000, leaves_lot = 50000.000000, price = 1.118520,' \
' set_time = 130797692543744290, change_time = 130797692543744290, status = 2, user_order_id = F-69287, aggr_src_name = RZBM, ' \
'avg_exec_price = 0.000000, show_lot = 1000000000000.000000, exec_type = 5, tif = 1, tif_time = 0, flags = 1, ' \
'comment_text = |FIX|, ver = 0, uid = 0, client_id = 183, min_lot = 0.000000'
def parse_aggr_id_set_change_time(deal, row):
    fill_method_execution_time(deal, row, 'Generate Order')
    aggr_id = int(re.search('aggr_id = (.+?),', row).group(1))
    set_time = int(re.search('set_time = (.+?),', row).group(1))
    change_time = int(re.search('set_time = (.+?),', row).group(1))
    deal.aggr_id, deal.set_time, deal.change_time = aggr_id, set_time, change_time

'00| 08:14:14.374 21595390.966573 ->   	 order_id: 10099850'
def parse_order_id(deal, row):
    fill_method_execution_time(deal, row, 'Generating Order Id')
    order_id = int(re.search('order_id: (.+?)', row).group(1))
    deal.order_id = order_id

'00| 08:14:14.374 21595390.966803 ->     selected group 8'
def parse_hedging_group(deal, row):
    fill_method_execution_time(deal, row, 'Choosing Hedging Group')
    group = int(re.search('selected group (.+?)', row).group(1))
    deal.set_hedging_group(group)


def fill_method_execution_time(deal, row, type_name):
    exact_time = float(row.split('->')[0].split(' ')[2])
    deal.methods_execution_time.add_sdt_method_execution_time(type_name, exact_time)

'00| 08:14:14.374 21595390.966549 ->   LocalOrders: Deal, LocalOrdersTypes::DEAL::REQ: FXBA::V2::DEAL::REQ: src_name = RZBM, ' \
'instr_name = EUR/USD, side = 0, lot = 50000.000000, price = 1.118520, user_deal_id = F-69287, client_id = 183, flags = 1, ' \
'checker1 = 933261842, stp_trader_name = , send_aggr_id = , ib_commission = 0.000000, comment_text = |FIX|, min_lot = 0.000000; ' \
'aggr_id = , order_id = , value_date = 1601/01/01, full_lot = 50 000, ib_profit_usd = 0.0, cl_aggr_id = , cl_req_price = 1.11852'
def parse_initial_order(row, year, month, day):
    if "LocalOrders: Deal, LocalOrdersTypes" in row:
        part = row.split('->')[0]
        first = part.split(' ')
        ms_time = datetime.strptime(first[1], '%H:%M:%S.%f').replace(year=year, month=month, day=day)
        exact_time = float(first[2])

        instrument = re.search('instr_name = (.+?),', row).group(1)
        side = re.search('side = (.+?),', row).group(1)
        requested_lot = float(re.search('lot = (.+?),', row).group(1))
        requested_price = float(re.search('price = (.+?),', row).group(1))
        used_deal_id = re.search('user_deal_id = (.+?),', row).group(1)
        client_id = int(re.search('client_id = (.+?),', row).group(1))
        flags = int(re.search('flags = (.+?),', row).group(1))
        comment_text = re.search('comment_text = (.+?),', row).group(1)
        min_lot = float(re.search('min_lot = (.+?);', row).group(1))
        initial_order = Order(ms_time, exact_time, client_id, used_deal_id, requested_lot, requested_price,
                              side, instrument, flags, comment_text, min_lot)
        return initial_order, True
    # elif "LocalOrders: order ready to exec, order_id" in row:
    #     '00| 15:19:12.043 21620887.964262 ->   LocalOrders: order ready to exec, order_id = 10114436'
    #     order_id = int(re.search('order_id = (.+?)', row).group(1))
    #     return Order(None, None, 1, None, None, None, None, None, None,
    #                  None, None), False, order_id
    else:
        return Order(None, None, 1, None, None, None, None, None, None,
                     None, None), False

if __name__ == "__main__":
    year = 2015
    month = 6
    day = 26
    client_logs_path = "HUY"
    client_parser(client_logs_path, year, month, day)


