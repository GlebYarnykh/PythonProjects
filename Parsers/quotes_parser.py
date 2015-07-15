import pandas as pd
import numpy as np
from dateutil import parser
import matplotlib.pyplot as plt
import pylab as pl
import matplotlib
from pandas.tools.plotting import table
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime
from pandas import ExcelWriter
from datetime import timedelta
from pandas.stats.moments import rolling_mean
from pandas import HDFStore
from glob import glob

output = 'F:\\DataBase\\BestBidAsk.h5'
pricing_store = HDFStore(output)

columns = ['a', 'Time', 'PreciseTime', 'delimiter', 'Group', 'Instrument', 'AvalBid', 'par2', 'par3', 'Bid', 'Vol_bid',
           'g', 'h', 'AvalAsk', 'par5', 'par6', 'Ask', 'Vol_ask', 'j']
col_to_use = ['Time', 'PreciseTime', 'Group', 'Instrument', 'AvalBid', 'Bid', 'AvalAsk', 'Ask']

""" 02| 23:59:59.328 21721400.597387 ->   56: AUD/USD: 1 0 8 0.74048000 1'000'000 1 / 1 1 8 0.74072000 1'000'000 1 """


def parse_client_prices(date):
    def date_parser(x, year, month, day):
        if '.' in x:
            time = datetime.strptime(x, '%H:%M:%S.%f')
        else:
            time = datetime.strptime(x, '%H:%M:%S')
        h = time.hour
        if (h >= 0) and (h < 23):
            return time.replace(year=year, month=month, day=day)
        else:
            return None

    def clean_instrument(x):
        return x.replace("/", "")[:-1]

    string_date = date.strftime("%Y.%m.%d")
    current_year = date.year
    current_month = date.month
    current_day = date.day
    path = 'M:\\logs\\cl_srv2-trade\\' + string_date + '\\mc_mngr_quotes.log'
    # clean existing copy of day (needed for parsing errors, etc..)
    # Parse day of quotes for all client groups
    if path in glob('M:\\logs\\cl_srv2-trade\\' + string_date + '\\*'):
        print(path, "Start Parsing Day", datetime.now())
        # pricing_store.append('ParsedDates', string_date + " started parsing", min_itemsize = {'values': 50})
        iterator = pd.read_csv(path, delim_whitespace=True, chunksize=5000000, header=None, names=columns,
                               usecols=col_to_use, thousands="'", error_bad_lines=False, warn_bad_lines=False)
        for chunk in iterator:
            chunk['Time'] = np.vectorize(date_parser)(chunk['Time'], current_year, current_month, current_day)
            chunk = chunk.loc[~((chunk['AvalBid'] == 0) & (chunk['AvalAsk'] == 0)) & ~(chunk['Time'].isin([None])) &
                              (chunk['Group'] != 'Error:')]
            chunk['Instrument'] = np.vectorize(clean_instrument)(chunk['Instrument'])
            if not chunk.empty:
                Instr = chunk['Instrument'].unique()
                chunk = chunk.set_index('Time')
                for inst in Instr:
                    chunk.loc[chunk.Instrument == inst].to_hdf(pricing_store, inst, append=True,
                                                               data_columns=['Group', 'Instrument'],
                                                               min_itemsize={'Instrument': 12},
                                                               complevel=9,
                                                               complib='zlib')

        print(path, "End Parsing Day", datetime.now())
        # pricing_store.append('ParsedDates', string_date + " ended parsing", min_itemsize = {'values': 50})


if __name__ == "__main__":
    start_date = datetime(2015, 4, 1)
    end_date = datetime(2015, 7, 13)
    date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days)
                      if (start_date + timedelta(days=x)).weekday() not in [5, 6]]
    for date in date_generated:
        parse_client_prices(date)
