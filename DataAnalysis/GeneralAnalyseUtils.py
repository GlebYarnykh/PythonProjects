from DataAnalysis.ReadDealsFromHDF import get_all_deals

__author__ = 'Gleb'

from datetime import timedelta, datetime

import pandas as pd
import numpy as np
from pandas import HDFStore
import matplotlib.pyplot as plt

prices = ['F:\\DataBase\\BestBidAsk.h5', 'F:\\DataBase\\ToxicBestBidAsk.h5']
good_prices_store = HDFStore(prices[0])
bad_prices_store = HDFStore(prices[1])

def get_prices(start_time, end_time, narrow, instrument):
    ccy_pair = '/' + instrument.split('/')[0] + instrument.split('/')[1]
    timestamp_end = pd.to_datetime(end_time)
    str_timestamp_end = str(timestamp_end)
    timestamp_start = pd.to_datetime(start_time)
    str_timestamp_start = str(timestamp_start)
    query = "index>Timestamp('" + str_timestamp_start + "') & index<Timestamp('" + str_timestamp_end +"')"
    if narrow == True:
        return good_prices_store.select(ccy_pair, where=query)
    else:
        return bad_prices_store.select(ccy_pair, where=query)

def get_standardized_prices(start_time, instrument, narrow, side):
    '''

    :param start_time: time when deal came to server
    :param short_time: time in seconds to look in future after the deal
    :param long_time: time in minutes to look in future after the deal (long history)
    :param instrument: instrument traded
    :param discretezation_level_short: '500ms'/'1s' format
    :param discretezation_level_long: '1s'/'10s' format
    :return: two series for bids and asks (one long, one short)
    '''
    start_time_long = start_time - np.timedelta64(60, 's')
    end_time_long = start_time + np.timedelta64(5, 'm')
    start_time_short = start_time - np.timedelta64(10, 's')
    end_time_short = start_time + np.timedelta64(30, 's')
    prices = get_prices(start_time_long-np.timedelta64(1,'m'), end_time_long + np.timedelta64(1,'m'), narrow, instrument)
    timestamps_short = pd.date_range(start_time_short, end_time_short, freq='100ms')
    ts = pd.Series(index=timestamps_short)
    interpolated_values_short = pd.concat([prices[['Ask','Bid']], ts]).sort_index()[['Bid', 'Ask']].interpolate('values').loc[ts.index].iloc[:401]
    timestamps_long = pd.date_range(start_time_long, end_time_long, freq='1s')
    ts = pd.Series(index=timestamps_long)
    interpolated_values_long = pd.concat([prices[['Ask','Bid']], ts]).sort_index()[['Bid', 'Ask']].interpolate('values').loc[ts.index].iloc[:361]
    interpolated_values_long = interpolated_values_long.iloc[:361]
    interpolated_values_short.index = np.arange(-10.0, 30.1, 0.1)
    interpolated_values_long.index = np.arange(-60.0, 301, 1)
    short_prices = (interpolated_values_short['Ask'] + interpolated_values_short['Bid'])/2
    long_prices = (interpolated_values_long['Ask'] + interpolated_values_long['Bid'])/2
    short_prices_final = (short_prices-short_prices.iloc[101])*100/short_prices.iloc[101]
    long_prices_final = (long_prices-long_prices.iloc[61])*100/long_prices.iloc[61]
    if side == 'Sell':
        return short_prices_final, long_prices_final
    else:
        return -short_prices_final, -long_prices_final

def get_price_action_quantiles(deals):
    short_prices = {}
    long_prices = {}
    for i, row in deals.iterrows():
        instrument = row['Instrument']
        time = row['Time']
        aggr_id = row['AggrId']
        side = row['Side']
        short_price, long_price = get_standardized_prices(time, instrument, True, side)
        short_prices[aggr_id] = short_price
        long_prices[aggr_id] = long_price
    short = pd.DataFrame(short_prices).T
    long = pd.DataFrame(long_prices).T
    return short,long

if __name__ == '__main__':
    cs = get_all_deals(True, datetime(2015,4,1), datetime(2015,7,10), 'vest.int')
    short_prices = {}
    long_prices = {}
    for i, row in cs.iterrows():
        instrument = row['Instrument']
        time = row['Time']
        aggr_id = row['AggrId']
        side = row['Side']
        short_price, long_price = get_standardized_prices(time, instrument, True, side)
        short_prices[aggr_id] = short_price
        long_prices[aggr_id] = long_price
    short = pd.DataFrame(short_prices).T
    long = pd.DataFrame(long_prices).T
    fig, axes = plt.subplots(1)
    short.quantile(0.7).plot(ax=axes, color='r')
    short.quantile(0.5).plot(ax=axes, color='g')
    short.quantile(0.3).plot(ax=axes, color='b')