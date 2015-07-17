from datetime import datetime, timedelta
from Parsers.ClientsLogsParser import client_parser

__author__ = 'GlebYarnykh'

import pandas as pd
import numpy as np
from pandas import HDFStore
import warnings

warnings.filterwarnings("ignore")
pd.set_option('io.hdf.default_format', 'table')
output = 'F:\\DataBase\\Deals.h5'
store = HDFStore(output)

client_only_non_mixed_types = ['OrderType', 'ExactTime', 'ClientName', 'Side', 'AggrId','Instrument', 'ReqLot',
                               'ReqPrice', 'MinLot', 'Flag',
                               'HedgingGroup', 'GroupId', 'BestAsk', 'BestBid', 'Tolerance', 'DoNotValidate', 'NoBetterPrices',
                               'QuoteLifetime', 'ValPrice', 'ExecLot', 'ExecPrice', 'SetTime', 'ChangeTime',
                               'Marination', 'UnexpectedMarination', 'Pnl', 'SoftPnl', 'NetPnl', 'EndOfDeal']
client_only_mixed_types = ['SdtMethodsExecution', 'BidQuotes', 'AskQuotes']

def parse_day_of_deals(date, detalization_level):
    day_deals = client_parser(date)
    # do not add deals
    # keys = get_existing_deals_ids(store)
    # day_deals = day_deals[~day_deals['AggrId'].isin(keys)]

    # current_ids = day_deals.loc[:, 'AggrId']
    # min_id = int(current_ids.min())
    # max_id = int(current_ids.max())
    # store.remove('Deals', where="(AggrId >= min_id) & (AggrId <= max_id)")
    store.append('Deals', day_deals[client_only_non_mixed_types], data_columns = True, complevel = 9, complib='zlib',min_itemsize = {'values': 50})
    for i, row in day_deals.iterrows():
        base_path = str(row['AggrId'])
        sdt_methods_execution = row['SdtMethodsExecution']
        store.append(base_path+'/SdtMethodsExecution', sdt_methods_execution, min_itemsize = {'values': 50}, complevel = 9, complib='zlib')
        bid_quotes = row['BidQuotes']
        store.append(base_path+'/BidQuotes', bid_quotes, min_itemsize = {'values': 50}, complevel = 9, complib='zlib')
        ask_quotes = row['AskQuotes']
        store.append(base_path+'/AskQuotes', ask_quotes, min_itemsize = {'values': 50}, complevel = 9, complib='zlib')
    # print(store)
    # store.close()


def get_existing_deals_ids(store):
    aggr_ids = store.keys()
    if '/Deals' in aggr_ids:
        aggr_ids.remove('/Deals')
    def local_id_parser(id):
        id = int(id.split('/')[1])
        return id
    aggr_ids_cleaned = [local_id_parser(id) for id in aggr_ids]
    return aggr_ids_cleaned

if __name__ == "__main__":
    start_date = datetime(2015, 4, 1)
    end_date = datetime(2015, 7, 11)
    date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days)
                      if (start_date + timedelta(days=x)).weekday() not in [5, 6]]
    for date in date_generated:
        parse_day_of_deals(date, "HUY")
