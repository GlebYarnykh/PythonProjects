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
    ask_path = '/' + str(aggr_id) + '/AskQuotes'

    bool = pd.to_datetime(index) > datetime(2015,4,25)
    if bool:
        try:
            bid_quotes = deals_store1.select(bid_path).drop_duplicates(subset='QuoteId', take_last=False)
            bid_aval = 1
        except KeyError:
            bid_quotes = pd.DataFrame()
            bid_aval = 0
        try:
            ask_quotes = deals_store1.select(ask_path).drop_duplicates(subset='QuoteId', take_last=False)
            ask_aval = 1
        except KeyError:
            ask_quotes = pd.DataFrame()
            ask_aval = 0
    else:
        try:
            bid_quotes = deals_store2.select(bid_path).drop_duplicates(subset='QuoteId', take_last=False)
            bid_aval = 1
        except KeyError:
            bid_quotes = pd.DataFrame()
            bid_aval = 0
        try:
            ask_quotes = deals_store2.select(ask_path).drop_duplicates(subset='QuoteId', take_last=False)
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


def get_all_deals(include_hedging_deals, global_start, global_cutoff, client_name):
    deals = pd.concat([deals_store1['Deals'], deals_store2['Deals']])
    deals['Time'] = deals.index
    deals = deals.loc[(deals['Time']<global_cutoff) & (deals['Time']>global_start) & (deals['ClientName']==client_name)]
    deals.sort('Time', ascending = False,inplace = True)
    deals.drop_duplicates('AggrId', take_last=True, inplace=True)

    deals['BidQuotes'], deals['AskQuotes'], deals['BidsAval'], deals['AsksAval'] = np.vectorize(get_client_order_book)(deals['AggrId'], deals['Time'])
    deals['BestSpread'] = deals['BestAsk'] - deals['BestBid']

    def get_date_info(date):
        true_date = pd.to_datetime(date)
        string_day = true_date.strftime('%Y.%m.%d')
        return true_date.year, true_date.month, true_date.day, true_date.hour, string_day
    deals['Year'], deals['Month'], deals['Day'], deals['Hour'], deals['StringDay'] = np.vectorize(get_date_info)(deals['Time'])

    def get_real_req_lot(aggr_id):
        req_size = sizes.loc[(sizes['Aggr Id']==aggr_id) & (sizes['Trader']!='DEALER'), 'Amount'].sum()
        if not sizes.loc[(sizes['Aggr Id']==aggr_id) & (sizes['Trader']!='DEALER') & (sizes['Result']=='Ok'), 'Amount'].empty:
            exec_size = sizes.loc[(sizes['Aggr Id']==aggr_id) & (sizes['Trader']!='DEALER') & (sizes['Result']=='Ok'), 'Amount'].sum()
        else:
            exec_size = 0.0
        return req_size, exec_size
    deals['ReqLot'], deals['ExecLot'] = np.vectorize(get_real_req_lot)(deals['AggrId'])

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
        timestamp_start = pd.to_datetime(time) - timedelta(days=1)
        str_timestamp_start = str(timestamp_start)
        query = "index>Timestamp('" + str_timestamp_start + "') & index<Timestamp('" + str_timestamp_end +"')"
        if ccy_pair in instruments:
            for_price = prices_store.select(ccy_pair, where=query).tail(1)
            try:
                convert_price = (for_price['Bid'] + for_price['Ask']).values[0]/2
            except IndexError:
                return ccy2_pnl/req_price
            return ccy2_pnl/convert_price
        elif swaped_ccy_pair in instruments:
            for_price = prices_store.select(swaped_ccy_pair, where=query).tail(1)
            try:
                convert_price = (for_price['Bid'] + for_price['Ask']).values[0]/2
            except IndexError:
                return ccy2_pnl*req_price
            return ccy2_pnl*convert_price
        else:
            print(time, instrument)
            return ccy2_pnl
    deals['Margin'] = np.vectorize(get_absolute_margin)(deals['ExecLot'], deals['ReqPrice'], deals['SoftPnl'], deals['Instrument'], deals['Time'])
    deals['Connection Type'] = deals['ClientName'].apply(lambda x: x.split('.')[1] if '.' in x else 'Manual')
    deals['HedgingGroup'] = np.vectorize(get_group_name)(deals['AggrId'], deals['HedgingGroup'])
    def find_similar_deal_soft_pnl(instrument, size, group_id, group, time, client):
        subset = deals.loc[(deals['Instrument']==instrument) &
                           (deals['ExecLot']>0) & (deals['ClientName']==client) & (deals['Time']<(pd.to_datetime(time)+timedelta(days=2)))].head(1)
        if subset.empty == True:
            subset = deals.loc[(deals['Instrument']==instrument) &
                           (deals['ExecLot']>0) & (deals['GroupId']==group_id) & (deals['Time']<pd.to_datetime(time))].head(1)
            if subset.empty == True:
                subset = deals.loc[(deals['Instrument']==instrument) &
                               (deals['ExecLot']>0) & (deals['HedgingGroup']==group) & (deals['Time']<pd.to_datetime(time))].head(1)
        try:
            margin = subset['Margin'].values[0]
        except IndexError:
            margin = np.nan
        return margin
    deals.loc[np.isnan(deals['Margin']), 'Margin'] = deals.loc[np.isnan(deals['Margin'])].apply(lambda row:
                                                                                                 find_similar_deal_soft_pnl(row['Instrument'], row['ReqLot'], row['GroupId'],
                                                                                                                            row['HedgingGroup'], row['Time'], row['ClientName']),
                                                                                                axis = 1)
    deals['BidForSize'], deals['AskForSize'], deals['TargetSize'] = np.vectorize(get_target_bidask)(deals['AggrId'], deals['ReqLot'], deals['BidQuotes'],
                                                                              deals['AskQuotes'], deals['BidsAval'], deals['AsksAval'], deals['BestAsk'], deals['BestBid'])
    if include_hedging_deals:
        def get_hedging_deals(aggr_id):
            LPs = sizes.loc[(sizes['Trader']=='DEALER')&(sizes['Aggr Id']==aggr_id), ['Source', 'Instrument', 'Amount', 'CCY1 Amount', 'Price', 'Time', 'Dealer Side (Pair)', 'Result']]
            return LPs.sort('Price', )
        deals['HedgingDeals'] = np.vectorize(get_hedging_deals)(deals['AggrId'])
    deals['DealTime'] = deals['EndOfDeal'] - deals['ExactTime']
    deals['SpreadForSize'] = deals['AskForSize'] - deals['BidForSize']
    deals['Count'] = 1
    deals['EBS_Indicator'] = deals.apply(lambda row: 1 if ('EBS' in row['HedgingDeals']['Source'].tolist()) else 0, axis=1)
    deals['IncomingSpread'] = deals['SpreadForSize'] - 2*deals['Margin']
    deals['NetMargin'] = deals['NetPnl']/deals['ExecLot']
    return deals

def get_target_bidask(aggr_id, size, bid_quotes, ask_quotes, bid_aval, ask_aval, best_bid, best_ask):
    def get_price_for_size_book(size, side):
        initial_size = 0.0
        initial_counter_size = 0.0
        for i, row in side.iterrows():
            initial_size += row['Size']
            initial_counter_size += row['Size']*row['Price']
            if initial_size>=size:
                diff = initial_size - size
                final_size = initial_size - diff
                find_counter_size = initial_counter_size - diff*row['Price']
                return find_counter_size/final_size, size
        return (side['Size']*side['Price']).sum()/side['Size'].sum(), size

    def get_price_for_size_bands(size, side):
        try:
            price = side.loc[(side['Size']>=size), 'Price'].head(1).values[0]
            target_size = side.loc[(side['Size']>=size), 'Size'].head(1).values[0]
        except:
            price = side['Price'].tail(1).values[0]
            target_size = size
        return price, target_size
    if bid_aval == 1:
        if bid_quotes['QuotingType'].values[0] == 'Book':
            bid_for_size, target_size = get_price_for_size_book(size, bid_quotes)
        else:
            bid_for_size, target_size = get_price_for_size_bands(size, bid_quotes)
    else:
        bid_for_size, target_size = best_bid, size
    if ask_aval == 1:
        if ask_quotes['QuotingType'].values[0] == 'Book':
            ask_for_size, target_size = get_price_for_size_book(size, ask_quotes)
        else:
            ask_for_size, target_size = get_price_for_size_bands(size, ask_quotes)
    else:
        ask_for_size, target_size = best_ask, size
    return bid_for_size, ask_for_size, target_size

def get_group_name(aggr_id, hedging_group):
    if hedging_group == 'nan':
        return sizes.loc[(sizes['Aggr Id']==aggr_id), 'Group Name'].values[0]
    else:
        return hedging_group





















