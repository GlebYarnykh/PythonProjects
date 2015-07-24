__author__ = 'Gleb'

from datetime import timedelta

import pandas as pd
import numpy as np
from pandas import HDFStore

prices = ['F:\\DataBase\\BestBidAsk.h5']
prices_store = HDFStore(prices[0])

def get_prices(start_time, end_time, instrument):
    ccy_pair = '/' + instrument.split('/')[0] + instrument.split('/')[1]
    timestamp_end = pd.to_datetime(end_time)
    str_timestamp_end = str(timestamp_end)
    timestamp_start = pd.to_datetime(start_time)
    str_timestamp_start = str(timestamp_start)
    query = "index>Timestamp('" + str_timestamp_start + "') & index<Timestamp('" + str_timestamp_end +"')"
    return prices_store.select(ccy_pair, where=query)

def get_standardized_prices(start_time, short_time, long_time, instrument, discretezation_level_short, discretezation_level_long):
    '''

    :param start_time: time when deal came to server
    :param short_time: time in seconds to look in future after the deal
    :param long_time: time in minutes to look in future after the deal (long history)
    :param instrument: instrument traded
    :param discretezation_level_short: '500ms'/'1s' format
    :param discretezation_level_long: '1s'/'10s' format
    :return: two series for bids and asks (one long, one short)
    '''
    start_time_long = start_time - timedelta(seconds=60)
    end_time_long = start_time + timedelta(minutes=long_time)
    start_time_short = start_time - timedelta(seconds=10)
    end_time_short = start_time + timedelta(seconds=short_time)
    prices = get_prices(start_time_long, end_time_long, instrument)
    timestamps_short = pd.date_range(start_time_short, end_time_short, freq=discretezation_level_short)
    ts = pd.Series(index=timestamps_short)
    interpolated_values_short = pd.concat([prices[['Ask','Bid']], ts]).sort_index()[['Bid', 'Ask']].interpolate('values').loc[ts.index]
    timestamps_long = pd.date_range(start_time_long, end_time_long, freq=discretezation_level_long)
    ts = pd.Series(index=timestamps_long)
    interpolated_values_long = pd.concat([prices[['Ask','Bid']], ts]).sort_index()[['Bid', 'Ask']].interpolate('values').loc[ts.index]
    interpolated_values_short['Time'] = interpolated_values_short.index
    interpolated_values_short.index = (interpolated_values_short['Time'] - pd.to_datetime(start_time)).apply(lambda x: x/np.timedelta64(1,'s'))
    del interpolated_values_short['Time']
    interpolated_values_long['Time'] = interpolated_values_long.index
    interpolated_values_long.index = (interpolated_values_long['Time'] - pd.to_datetime(start_time)).apply(lambda x: x/np.timedelta64(1,'s'))
    del interpolated_values_long['Time']
    return interpolated_values_short, interpolated_values_long
