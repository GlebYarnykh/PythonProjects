import numpy as np
import pandas as pd
from Parsers.ClientsLogsParser import manager_deals
from Parsers.Rows_parsers import parse_datetime

__author__ = 'Gleb'


def get_manager_deals():
    path_for_local_manager_deals = "C:\\Logs Examples\\PartDeals.csv"
    mngr_deals = pd.read_csv(path_for_local_manager_deals, sep=";", decimal=',', na_values='-')
    return mngr_deals


def get_limit_orders_file_sdt():
    # Preparing aggregator data
    path_for_limit_orders = "C:\\Logs Examples\\LimitOrders.csv"
    orders = pd.read_csv(path_for_limit_orders, sep=";", decimal=',', na_values='-')
    orders['Set Time'], orders['year'], orders['month'], orders['day'] = np.vectorize(parse_datetime)(
        orders['Set Time'])
    orders['Change Time'], orders['year'], orders['month'], orders['day'] = np.vectorize(parse_datetime)(
        orders['Change Time'])
    orders['Lot'] = orders['Lot'].apply(float)
    orders['ExecLot'] = orders['ExecLot'].apply(float)
    orders['Price'] = orders['Price'].apply(float)
    orders['AvgExecPrice'] = orders['AvgExecPrice'].apply(float)

    return orders


def get_pnls(aggr_id):
    print(aggr_id)
    pnl = manager_deals.loc[manager_deals['Aggr Id']==str(aggr_id), 'Profit']
    if not pnl.empty:
        pnl_final = pnl.values[0]
    else:
        pnl_final = np.nan
    soft_pnl = manager_deals.loc[manager_deals['Aggr Id']==str(aggr_id), 'SoftPnL(USD)']
    if not soft_pnl.empty:
        soft_pnl_final = soft_pnl.values[0]
    else:
        soft_pnl_final = np.nan
    return pnl_final, soft_pnl_final