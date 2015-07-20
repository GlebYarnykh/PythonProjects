from datetime import datetime
import re
from CoreObjects.Order import Order

__author__ = 'Gleb'


def parse_initial_limit_order(order_id, limit_orders, local_orders, year,month,day):
    limit_index = local_orders.loc[local_orders['OrderIndex'] == order_id].index.max()
    row = local_orders.at[limit_index, 0]
    part = row.split('->')[0]
    first = part.split(' ')
    ms_time = datetime.strptime(first[1], '%H:%M:%S.%f').replace(year=year, month=month, day=day)
    exact_time = float(first[2])
    order_type = 'Limit'
    instrument = limit_orders.loc[(limit_orders['Order Id'] == order_id), 'Instr Name'].values[0]
    side = limit_orders.loc[(limit_orders['Order Id'] == order_id), 'Side'].values[0]
    client_id = limit_orders.loc[(limit_orders['Order Id'] == order_id), 'Trader'].values[0]
    requested_lot = limit_orders.loc[(limit_orders['Order Id'] == order_id), 'Lot'].values[0]
    requested_price = limit_orders.loc[(limit_orders['Order Id'] == order_id), 'Price'].values[0]
    comment_text = limit_orders.loc[(limit_orders['Order Id'] == order_id), 'Comment'].values[0]
    minimum_lot = 0.0
    flags = limit_orders.loc[(limit_orders['Order Id'] == order_id), 'Flags'].values[0]
    order = Order(order_type, ms_time, exact_time, client_id, 0, requested_lot, requested_price,
                  side, instrument, flags, comment_text, minimum_lot)
    return order, limit_index


def parse_initial_ioc_order(row, year, month, day):
    if 'i00|' in row:
        return 'Skip'
    part = row.split('->')[0]
    first = part.split(' ')
    ms_time = datetime.strptime(first[1], '%H:%M:%S.%f').replace(year=year, month=month, day=day)
    exact_time = float(first[2])

    instrument = re.search('instr_name = (.+?),', row).group(1)
    side = int(re.search('side = (.+?),', row).group(1))
    requested_lot = float(re.search('lot = (.+?),', row).group(1))
    requested_price = float(re.search('price = (.+?),', row).group(1))
    user_deal_id = re.search('user_deal_id = (.+?),', row).group(1)
    client_id = int(re.search('client_id = (.+?),', row).group(1))
    flags = int(re.search('flags = (.+?),', row).group(1))
    comment_text = re.search('comment_text = (.+?),', row).group(1)
    min_lot = float(re.search('min_lot = (.+?);', row).group(1))
    initial_order = Order('IOC', ms_time, exact_time, client_id, user_deal_id, requested_lot, requested_price,
                          side, instrument, flags, comment_text, min_lot)
    return initial_order