from CoreObjects.Order import Order

__author__ = 'ruayhg'

import pandas as pd
import numpy as np
from datetime import datetime
import re

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

        instrument = re.findall('instr_name = (.+?),', row)[0]
        side = re.findall('side = (.+?),', row)[0]
        requested_lot = float(re.findall('lot = (.+?),', row)[0])
        requested_price = float(re.findall('price = (.+?),', row)[0])
        used_deal_id = re.findall('user_deal_id = (.+?),', row)[0]
        client_id = int(re.findall('client_id = (.+?),', row)[0])
        flags = int(re.findall('flags = (.+?),', row)[0])
        comment_text = re.findall('comment_text = (.+?),', row)[0]
        min_lot = float(re.findall('min_lot = (.+?);', row)[0])
        initial_order = Order(ms_time, exact_time, client_id, used_deal_id, requested_lot, requested_price,
                              side, instrument, flags, comment_text, min_lot)
        return initial_order
    else:
        return None

if __name__ == "__main__":
    path = "C:\\Users\\ruayhg\\PycharmProjects\\BigLogs\\local_orders.log"
    local_order = pd.read_csv(path, sep=":;")
    local_order['Order'] = np.vectorize(parse_initial_order)(local_order[0], 2015, 6, 26)

