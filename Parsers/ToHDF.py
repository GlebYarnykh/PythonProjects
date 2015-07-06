from datetime import datetime
from Parsers.ClientsLogsParser import client_parser

__author__ = 'GlebYarnykh'

import pandas as pd
import numpy as np
from pandas import HDFStore

pd.set_option('io.hdf.default_format', 'table')

# date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]
output = 'C:\\Users\\ruayhg\\PycharmProjects\\BigLogs\\Deals.h5'
store = HDFStore(output)

client_only_non_mixed_types = ['ExactTime', 'ClientName', 'Side', 'Instrument', 'ReqLot', 'ReqPrice', 'MinLot', 'Flag',
                               'HedgingGroup', 'BestAsk', 'BestBid', 'Tolerance', 'DoNotValidate', 'NoBetterPrices',
                               'QuoteLifetime', 'ValPrice', 'ExecLot', 'ExecPrice']
client_only_mixed_types = ['SdtMethodsExecution', 'BidQuotes', 'AskQuotes']

def parse_day_of_deals(date, detalization_level):
    day_deals = client_parser("HUY", date.year, date.month, date.day)
    store.append('Deals', day_deals[client_only_non_mixed_types])
    for i, row in day_deals.iterrows():
        base_path = str(row['AggrId'])
        store.append(base_path+'/SdtMethodsExecution', row['SdtMethodsExecution'])
        store.append(base_path+'/BidQuotes', row['BidQuotes'])
        store.append(base_path+'/AskQuotes', row['AskQuotes'])
    print(store)

if __name__ == "__main__":
    parse_day_of_deals(datetime(2015, 6, 26), "HUY")
