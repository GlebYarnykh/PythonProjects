from datetime import datetime
from Parsers.ClientsLogsParser import client_parser

__author__ = 'GlebYarnykh'

import pandas as pd
import numpy as np
from pandas import HDFStore

pd.set_option('io.hdf.default_format', 'table')
output = 'F:\\DataBase\\Deals.h5'
store = HDFStore(output)
# date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]


client_only_non_mixed_types = ['ExactTime', 'ClientName', 'Side', 'AggrId','Instrument', 'ReqLot', 'ReqPrice', 'MinLot', 'Flag',
                               'HedgingGroup', 'BestAsk', 'BestBid', 'Tolerance', 'DoNotValidate', 'NoBetterPrices',
                               'QuoteLifetime', 'ValPrice', 'ExecLot', 'ExecPrice']
client_only_mixed_types = ['SdtMethodsExecution', 'BidQuotes', 'AskQuotes']

def parse_day_of_deals(date, detalization_level):
    day_deals = client_parser("HUY", date.year, date.month, date.day)
    current_ids = day_deals.loc[:, 'AggrId']
    min_id = int(current_ids.min())
    max_id = int(current_ids.max())
    if '/Deals' in store.keys():
        store.remove('Deals', where="(AggrId >= min_id) & (AggrId <= max_id)")
    store.append('Deals', day_deals[client_only_non_mixed_types], data_columns = True)
    for i, row in day_deals.iterrows():
        base_path = str(row['AggrId'])
        sdt_methods_execution = row['SdtMethodsExecution']
        store.append(base_path+'/SdtMethodsExecution', sdt_methods_execution)
        bid_quotes = row['BidQuotes']
        store.append(base_path+'/BidQuotes', bid_quotes)
        ask_quotes = row['AskQuotes']
        store.append(base_path+'/AskQuotes', ask_quotes)
    print(store)
    store.close()

if __name__ == "__main__":
    parse_day_of_deals(datetime(2015, 6, 26), "HUY")
