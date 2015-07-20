from Parsers.ClientsLogsParser import comes_sdt
from Parsers.DataMerger import Data_merger

__author__ = 'Gleb'
import warnings
from datetime import timedelta
from datetime import datetime

import pandas as pd
import numpy as np
from pandas import HDFStore

warnings.filterwarnings("ignore")
prices = ['F:\\DataBase\\BestBidAsk.h5']
deals = ['F:\\DataBase\\DealsFrom27Apr.h5', 'F:\\DataBase\\Deals.h5']

prices_store = HDFStore(prices[0])
instruments = prices_store.keys()
deals_store1 = HDFStore(deals[0])
deals_store2 = HDFStore(deals[1])
sizes = Data_merger('Deals', ' Nonaggr.csv')

def get_client_order_book(aggr_id, index):
    bid_path = '/' + str(aggr_id) + '/BidQuotes'
    ask_path = '/' + str(aggr_id) + '/BidQuotes'

    bool = pd.to_datetime(index) > datetime(2015,4,25)
    if bool:
        try:
            bid_quotes = deals_store1.select(bid_path)
            bid_aval = 1
        except KeyError:
            bid_quotes = pd.DataFrame()
            bid_aval = 0
        try:
            ask_quotes = deals_store1.select(ask_path)
            ask_aval = 1
        except KeyError:
            ask_quotes = pd.DataFrame()
            ask_aval = 0
    else:
        try:
            bid_quotes = deals_store2.select(bid_path)
            bid_aval = 1
        except KeyError:
            bid_quotes = pd.DataFrame()
            bid_aval = 0
        try:
            ask_quotes = deals_store2.select(ask_path)
            ask_aval = 1
        except KeyError:
            ask_quotes = pd.DataFrame()
            ask_aval = 0
    return bid_quotes, ask_quotes, bid_aval, ask_aval


def get_expected_range(size):
    if size<=1000000:
        return 0.0, 1000000
    elif size<=3000000:
        return 1000001, 3000000
    elif size<=5000000:
        return 3000001, 5000000
    else:
        return 5000001, 50000000


def get_all_deals(include_price_action, global_start, global_cutoff):
    deals = pd.concat([deals_store1['Deals'], deals_store2['Deals']])
    deals['Time'] = deals.index
    deals = deals.loc[(deals['Time']<global_cutoff) & (deals['Time']>global_start)]
    deals.sort('Time', ascending = False,inplace = True)
    deals.drop_duplicates('AggrId', take_last=True, inplace=True)
    deals['BidQuotes'], deals['AskQuotes'], deals['BidsAval'], deals['AsksAval'] = np.vectorize(get_client_order_book)(deals['AggrId'], deals['Time'])
    deals['BestSpread'] = deals['BestAsk'] - deals['BestBid']
    deals['Year'], deals['Month'], deals['Day'], deals['Hour'], deals['StringDay'] = np.vectorize(get_date_info)(deals['Time'])
    deals['ReqLot'], deals['ExecLot'] = np.vectorize(get_real_req_lot)(deals['AggrId'])
    deals['Margin'] = np.vectorize(get_absolute_margin)(deals['ExecLot'], deals['ReqPrice'], deals['SoftPnl'], deals['Instrument'], deals['Time'])
    deals['Connection Type'] = deals['ClientName'].apply(lambda x: x.split('.')[1] if '.' in x else 'Manual')
    deals['HedgingGroup'] = np.vectorize(get_group_name)(deals['AggrId'], deals['HedgingGroup'])
    def find_similar_deal_soft_pnl(instrument, size, group_id, group, time):
        start_of_bin, end_of_bin = get_expected_range(size)
        subset = deals.loc[(deals['Instrument']==instrument) & (deals['ExecLot']<=end_of_bin) & (deals['ExecLot']>=start_of_bin) &
                           (deals['ExecLot']>0) & (deals['HedgingGroup']==group) & (deals['Time']<pd.to_datetime(time))].head(1)
        if subset.empty == True:
            print(pd.to_datetime(time), instrument, group, size)
            subset = deals.loc[(deals['Instrument']==instrument) &
                           (deals['ExecLot']>0) & (deals['GroupId']==group_id) & (deals['Time']<pd.to_datetime(time))].head(1)
        try:
            margin = subset['Margin'].values[0]
        except IndexError:
            print('SUKA NET SDELOK')
            margin = np.nan
        return margin
    deals.loc[np.isnan(deals['Margin']), 'Margin'] = deals.loc[np.isnan(deals['Margin'])].apply(lambda row:
                                                                                                 find_similar_deal_soft_pnl(row['Instrument'], row['ReqLot'], row['GroupId'],
                                                                                                                            row['HedgingGroup'], row['Time']),
                                                                                                axis = 1)
    return deals

def get_real_req_lot(aggr_id):
    req_size = sizes.loc[(sizes['Aggr Id']==aggr_id) & (sizes['Trader']!='DEALER'), 'Amount'].sum()
    if not sizes.loc[(sizes['Aggr Id']==aggr_id) & (sizes['Trader']!='DEALER') & (sizes['Result']=='Ok'), 'Amount'].empty:
        exec_size = sizes.loc[(sizes['Aggr Id']==aggr_id) & (sizes['Trader']!='DEALER') & (sizes['Result']=='Ok'), 'Amount'].sum()
    else:
        exec_size = 0.0
    return req_size, exec_size

def get_group_name(aggr_id, hedging_group):
    if hedging_group == 'nan':
        return sizes.loc[(sizes['Aggr Id']==aggr_id), 'Group Name'].values[0]
    else:
        return hedging_group

def get_date_info(date):
    true_date = pd.to_datetime(date)
    string_day = true_date.strftime('%Y.%m.%d')
    return true_date.year, true_date.month, true_date.day, true_date.hour, string_day

def get_absolute_margin(exec_lot, req_price, soft_pnl, instrument, time):
    if exec_lot == 0.0:
        return np.nan
    ccy1 = instrument.split('/')[0]
    ccy2 = instrument.split('/')[1]
    ccy2_pnl = soft_pnl/exec_lot
    if (ccy2 == 'USD') or (ccy2 == 'USD_TOM') or (ccy2 == 'USD_TOD'):
        return ccy2_pnl
    if (ccy1 == 'USD'):
        return ccy2_pnl*req_price
    ccy_pair = ('/' + ccy2 + 'USD')
    swaped_ccy_pair = ('/' + 'USD' + ccy2)
    timestamp_end = pd.to_datetime(time)
    str_timestamp_end = str(timestamp_end)
    timestamp_start = pd.to_datetime(time) - timedelta(hours = 3)
    str_timestamp_start = str(timestamp_start)
    query = "index>Timestamp('" + str_timestamp_start + "') & index<Timestamp('" + str_timestamp_end +"')"
    if ccy_pair in instruments:
        for_price = prices_store.select(ccy_pair, where=query).tail(1)
        try:
            convert_price = (for_price['Bid'] + for_price['Ask']).values[0]/2
        except IndexError:
            return ccy2_pnl*req_price
        return ccy2_pnl*convert_price
    elif swaped_ccy_pair in instruments:
        for_price = prices_store.select(swaped_ccy_pair, where=query).tail(1)
        try:
            convert_price = (for_price['Bid'] + for_price['Ask']).values[0]/2
        except IndexError:
            return ccy2_pnl/req_price
        return ccy2_pnl/convert_price
    else:
        print(time, instrument)
        return ccy2_pnl

















