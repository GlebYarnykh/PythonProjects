from CoreObjects.Deal import Deal
from Parsers.Index_parsers import parse_ioc_index, parse_limit_index, parse_marination_index
from Parsers.Initial_parsers import parse_initial_limit_order, parse_initial_ioc_order
from Parsers.Rows_parsers import fill_client_deal_part
from Parsers.SdtManagerFiles import get_manager_deals, get_limit_orders_file_sdt, get_comission_file_sdt

__author__ = 'ruayhg'

import pandas as pd
import numpy as np
import re

manager_deals = get_manager_deals()
orders_sdt = get_limit_orders_file_sdt()
comes_sdt = get_comission_file_sdt()

def client_parser(date):
    string_date = date.strftime("%Y.%m.%d")
    year = date.year
    month = date.month
    day = date.day
    general_path = 'M:\\logs\\cl_srv2-trade\\' + string_date
    # Get Local_orders file
    local_orders = get_local_orders(general_path)

    limit_orders_sdt = orders_sdt.loc[(orders_sdt['Exec Type'] == 'Limit Order') &
                            (orders_sdt['State'].isin(['Deleted', 'Executed'])) &
                            (orders_sdt['year'] == year) &
                            (orders_sdt['month'] == month) &
                            (orders_sdt['day'] == day), :]
    # Parse Marination Time
    marination_times = get_marination_times(general_path, year, month, day)# Parse main client_deal parameters
    # Parse Limit Orders
    limit_orders = get_limit_orders(limit_orders_sdt, local_orders, year,month,day)
    # Parse IOC Orders
    ioc_orders = get_ioc_orders(local_orders, marination_times, year, month, day)
    # Merge limit and ioc orders
    deals = pd.concat([ioc_orders, limit_orders])
    # Get PNL, SoftPNL from FX Aggregator
    if not deals.empty:
        deals['Pnl'], deals['SoftPnl'], deals['NetPnl'] = np.vectorize(get_pnls)(deals['AggrId'])
        deals = deals.convert_objects()
        deals[['Pnl', 'SoftPnl', 'NetPnl']] = deals[['Pnl', 'SoftPnl', 'NetPnl']].fillna(0.0)
    return deals

def get_local_orders(general_path):
    path_for_local_orders_log = general_path + "\\local_orders.log"
    local_orders = pd.read_csv(path_for_local_orders_log, sep=":;", header=None)
    local_orders.dropna(how='all', inplace=True)
    local_orders.reset_index(inplace=True)
    # Get Limit/IOC Indices
    local_orders['IOCIndex'] = np.vectorize(parse_ioc_index)(local_orders[0], local_orders.index)
    local_orders['OrderIndex'] = np.vectorize(parse_limit_index)(local_orders[0])
    return local_orders


def get_limit_orders(limit_orders, local_orders, year, month, day):
    # Preparing logs data
    limit_ids = limit_orders.loc[limit_orders['State'] == 'Executed', 'Order Id'].values
    deal_storage = {}
    for order_id in limit_ids:
        order, index_id = parse_initial_limit_order(order_id, limit_orders, local_orders, year,month,day)
        deal = Deal(order, 0.0, 0.0)
        deal_array = local_orders.loc[index_id:(index_id + 500), 0]
        for j, row in deal_array.iteritems():
            status = fill_client_deal_part(row, deal)
            if status == 'End of Deal':
                print(status, deal.order.ms_time, deal.aggr_id)
                deal.executed_lot = limit_orders.loc[limit_orders['Order Id'] == order_id, 'ExecLot'].values[0]
                deal.executed_price = limit_orders.loc[limit_orders['Order Id'] == order_id, 'AvgExecPrice'].values[0]
                deal_storage[deal.order.ms_time] = deal.to_pandas_series_client_data_only()
                break
            elif j == 499 and deal.executed_lot is np.nan:
                print("Need more raws")
                print(row)
    data_matrix = pd.DataFrame(deal_storage).T.convert_objects()
    return data_matrix

def get_ioc_orders(local_orders, marination_times, year, month, day):
    order_indices = local_orders[local_orders['IOCIndex'] != 0].index
    deal_storage = {}
    for starting_index in order_indices:
        order = parse_initial_ioc_order(local_orders.loc[starting_index, 0], year, month, day)
        if order == 'Skip':
            continue
        if order.user_deal_id in marination_times.keys():
            marination_time = marination_times[order.user_deal_id]
        else:
            marination_time = (0.0,0.0)
        deal = Deal(order, marination_time[0], marination_time[1])
        if deal.order.ms_time in deal_storage.keys():
            continue
        deal_array = local_orders.loc[starting_index:(starting_index + 500), 0]
        for j, row in deal_array.iteritems():
            status = fill_client_deal_part(row, deal)
            if status == 'End of Deal':
                print(status, deal.order.ms_time, deal.aggr_id)
                deal_storage[deal.order.ms_time] = deal.to_pandas_series_client_data_only()
                break
            elif j == 499 and deal.executed_lot is np.nan:
                print("Need more rows")
                print(row)

    data_matrix = pd.DataFrame(deal_storage).T.convert_objects()
    return data_matrix

def get_marination_times(path_for_marination, year, month, day):
    path_for_marination = path_for_marination + '\\mc_mngr.log'
    marination = pd.read_csv(path_for_marination, sep=":;", header=None)
    marination.dropna(how="all", inplace=True)
    marination['OrderIndex'] = np.vectorize(parse_marination_index)(marination[0], marination.index)
    marination.reset_index(inplace=True)
    marination_indices = marination[marination['OrderIndex'] != 0].index
    marination_storage = {}
    for starting_index in marination_indices:
        row = marination.at[starting_index, 0]
        user_deal_id = re.search('user_deal_id = (.+?),', row).group(1)
        if not 's' in row.split('->')[0].split(' ')[2]:
            start = float(row.split('->')[0].split(' ')[2])
        else:
            continue
        end_index = min(starting_index + 500, marination.index.max())
        subsequent_array = marination.loc[starting_index:end_index, 0]
        for i, row in subsequent_array.iteritems():
            if "Try Deal: FXBA::V2::DEAL::REQ:" in row:
                end = float(row.split('->')[0].split(' ')[2])
            elif 'Sending deal to _local_orders->deal' in row:
                unexpected_end = float(row.split('->')[0].split(' ')[2])
                break
            else:
                continue
        marination_time = (end - start)*1000
        real_marination_time = (unexpected_end - end)*1000
        marination_storage[user_deal_id] = marination_time, real_marination_time
    return marination_storage

def get_pnls(aggr_id):
    pnl = manager_deals.loc[manager_deals['Aggr Id'] == aggr_id, 'Profit']
    if not pnl.empty:
        pnl_final = float(pnl.values[0])
    else:
        pnl_final = 0.0
    soft_pnl = manager_deals.loc[manager_deals['Aggr Id'] == aggr_id, 'SoftPnL(USD)']
    if not soft_pnl.empty:
        soft_pnl_final = float(soft_pnl.values[0])
    else:
        soft_pnl_final = 0.0
    if aggr_id in comes_sdt.index.tolist():
        comes = comes_sdt.loc[aggr_id]
        net_pnl = pnl_final - comes
    else:
        net_pnl = pnl_final
    return pnl_final, soft_pnl_final, net_pnl

'00| 09:53:58.249 21601374.672032 ->   LocalOrders: ---- FXBA::V2::ORDER::ENTRY: id = 10100160, action = 1, ' \
'instr_name = USD/RUB_TOM, aggr_id = 10100160, external_id = , ifoco_params = , src_name = RZBM, trader = psbf.api,' \
' side = 1, lot = 500000.000000, executed_lot = 0.000000, leaves_lot = 500000.000000, price = 54.837000, ' \
'set_time = 130797752372358140, change_time = 130797752372358140, status = 2, user_order_id = F2-153583, ' \
'aggr_src_name = RZBM, avg_exec_price = 0.000000, show_lot = 1000000000000.000000, exec_type = 5, tif = 1, ' \
'tif_time = 0, flags = 0, comment_text = |FIX|, ver = 0, uid = 0, client_id = 109, min_lot = 0.000000'

'00| 19:56:47.428 21637542.910265 ->   	don\'t validate price'

'00| 08:14:14.374 21595390.966858 ->   bid  : exist = 1, FXBA::V2::BOOK2::ENTRY: id = 0, action = 8, type = 0, ' \
'price = 1.118420, volume = 500000.000000, flags = 1, ord_data = vector, 0 entries'

'00| 08:14:14.390 21595390.966991 ->   FXBA::V2::BOOK2::ENTRY: id = 0, action = 8, type = 0, price = 1.118420, ' \
'volume = 500000.000000, flags = 1, ord_data = vector, 0 entries'

'00| 08:14:14.374 21595390.966874 ->   price_tolerance = 0.000005'

'00| 08:14:14.390 21595390.967246 ->   	process no better prices'

'00| 09:53:57.235 21601373.667698 ->   LocalOrders: src deal: quote time (ms): 1981'

'00| 08:14:14.374 21595390.966659 ->   LocalOrders: SendOrderToSubscribers, action: 1, FXBA::V2::ORDER::ENTRY:' \
' id = 10099850, action = 1, instr_name = EUR/USD, aggr_id = 10099850, external_id = , ifoco_params = , src_name = RZBM, ' \
'trader = acbk.bbg, side = 0, lot = 50000.000000, executed_lot = 0.000000, leaves_lot = 50000.000000, price = 1.118520,' \
' set_time = 130797692543744290, change_time = 130797692543744290, status = 2, user_order_id = F-69287, aggr_src_name = RZBM, ' \
'avg_exec_price = 0.000000, show_lot = 1000000000000.000000, exec_type = 5, tif = 1, tif_time = 0, flags = 1, ' \
'comment_text = |FIX|, ver = 0, uid = 0, client_id = 183, min_lot = 0.000000'

'00| 08:14:14.374 21595390.966573 ->   	 order_id: 10099850'

'00| 08:14:14.374 21595390.966803 ->     selected group 8'

'00| 08:14:14.374 21595390.966549 ->   LocalOrders: Deal, LocalOrdersTypes::DEAL::REQ: FXBA::V2::DEAL::REQ: src_name = RZBM, ' \
'instr_name = EUR/USD, side = 0, lot = 50000.000000, price = 1.118520, user_deal_id = F-69287, client_id = 183, flags = 1, ' \
'checker1 = 933261842, stp_trader_name = , send_aggr_id = , ib_commission = 0.000000, comment_text = |FIX|, min_lot = 0.000000; ' \
'aggr_id = , order_id = , value_date = 1601/01/01, full_lot = 50 000, ib_profit_usd = 0.0, cl_aggr_id = , cl_req_price = 1.11852'

'00| 18:41:28.474 21633024.072968 ->   LocalOrders: order ready to exec, order_id = 10117400'


# if __name__ == "__main__":
#     year = 2015
#     month = 6
#     day = 26
#     client_logs_path = "HUY"
#     sample_deals = client_parser(client_logs_path, year, month, day)
