import numpy as np
import pandas as pd
from Parsers.DataMerger import Data_merger, comission_array
from Parsers.Rows_parsers import parse_datetime

__author__ = 'Gleb'

def get_manager_deals():
    mngr_deals = Data_merger('Deals', ' Deals.csv')
    return mngr_deals

def get_limit_orders_file_sdt():
    # Preparing aggregator data
    orders = Data_merger('Orders', ' Orders.csv')
    orders['Set Time'], orders['year'], orders['month'], orders['day'] = np.vectorize(parse_datetime)(
        orders['Set Time'])
    orders['Change Time'], orders['year'], orders['month'], orders['day'] = np.vectorize(parse_datetime)(
        orders['Change Time'])
    orders['Lot'] = orders['Lot'].apply(float)
    orders['ExecLot'] = orders['ExecLot'].apply(float)
    orders['Price'] = orders['Price'].apply(float)
    orders['AvgExecPrice'] = orders['AvgExecPrice'].apply(float)

    return orders

def get_comission_file_sdt():
    nonaggr = Data_merger('Deals', ' Nonaggr.csv')
    comes = comission_array(nonaggr)
    return comes

