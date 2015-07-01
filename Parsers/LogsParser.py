from CoreObjects.Deal import Deal
from CoreObjects.Order import Order

__author__ = 'ruayhg'

import pandas as pd
import numpy as np
from datetime import datetime
import re



def client_parser(client_logs_path, year, month, day):
    path = "C:\\Users\\ruayhg\\PycharmProjects\\BigLogs\\local_orders.log"
    local_orders = pd.read_csv(path, sep=":;", header=None)
    local_orders['Order'] = np.vectorize(parse_initial_order)(local_orders[0], year, month, day)
    order_indices = local_orders[local_orders['Order'] != 'False'].index
    for starting_index in order_indices:
        order = local_order.loc[starting_index, 'Order']
        deal = Deal(order)
        deal_array = local_orders.loc[starting_index:(starting_index+100), 0]
        for j, row in deal_array.iterrows():
            fill_client_deal_part(row, deal)


def fill_client_deal_part(row, deal):
    if ('selected group' in row) and (deal.hedging_group is None):
        parse_hedging_group(deal, row)
    elif ('order_id: ' in row) and (deal.order_id is None):
        parse_order_id(deal, row)

'00| 08:14:14.374 21595390.966573 ->   	 order_id: 10099850'
def parse_order_id(deal, row):
    order_id = int(re.search('order_id: (.+?)', row))
    deal.order_id = order_id

'00| 08:14:14.374 21595390.966803 ->     selected group 8'
def parse_hedging_group(deal, row):
    exact_time = float(row.split('->')[0].split(' ')[2])
    deal.add_sdt_method_execution_time({'Entry type': 'selecting group', 'Time': exact_time})
    group = int(re.findall('selected group (.+?)', row)[0])
    deal.set_hedging_group(group)


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

        instrument = re.search('instr_name = (.+?),', row)
        side = re.search('side = (.+?),', row)
        requested_lot = float(re.search('lot = (.+?),', row))
        requested_price = float(re.search('price = (.+?),', row))
        used_deal_id = int(re.search('user_deal_id = (.+?),', row))
        client_id = int(re.search('client_id = (.+?),', row))
        flags = int(re.search('flags = (.+?),', row))
        comment_text = re.search('comment_text = (.+?),', row)
        min_lot = float(re.search('min_lot = (.+?);', row))
        initial_order = Order(ms_time, exact_time, client_id, used_deal_id, requested_lot, requested_price,
                              side, instrument, flags, comment_text, min_lot)
        return initial_order
    else:
        return 'False'

if __name__ == "__main__":
    path = "C:\\Users\\ruayhg\\PycharmProjects\\BigLogs\\local_orders.log"
    local_order = pd.read_csv(path, sep=":;", header=None)
    local_order['Order'] = np.vectorize(parse_initial_order)(local_order[0], 2015, 6, 26)
    local_order.loc[local_order['Order'] != 'False'].head()

