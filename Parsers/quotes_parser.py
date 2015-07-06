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


def Best_prices_parser(start_date, end_date):    

    date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]
    output = 'F:\\DataBase\\Prices\\BestBidAsk.h5'
    store = HDFStore(output)
    
    def date_parser(x, year, month, day):
        time = datetime.strptime(x, '%H:%M:%S.%f')
        h = time.hour
        if (h>6) and (h<23):
            return time.replace(year=year, month=month, day=day)
        else:
            return None
            
    def Clean_instrument(x):
        return x.replace("/","")[:-1]         
                  
    for i in date_generated:
        
        date = i.strftime("%Y.%m.%d")
        current_year = int(date[:4])
        current_month = int(date[5:7])
        current_day = int(date[8:10])
        print(i)
        path  = 'M:\\logs\\cl_srv2-trade\\' + date + '\\mc_mngr_quotes.log'
        
        if path in glob('M:\\logs\\cl_srv2-trade\\' + date +'\\*'):
            columns = ['a','Time','PreciseTime','b','Group', 'Instrument','par1','par2','par3','Bid','Vol_bid','g', 'h', 'par4','par5','par6','Ask','Vol_ask','j']
            col_to_use = ['Time', 'PreciseTime', 'Group', 'Instrument', 'Bid', 'Ask']
            iterator = pd.read_csv(path, delim_whitespace=True, chunksize=1000000, header=None, names=columns, usecols=col_to_use, thousands="'",error_bad_lines=False, warn_bad_lines=False)
            
            for j in iterator:
                j['Time'] = np.vectorize(date_parser)(j['Time'], current_year, current_month, current_day)                                 
                j = j[~j.Time.isin([None]) & j.Group.isin(['25:'])]
                if j.empty == False:
                    j['Instrument'] = np.vectorize(Clean_instrument)(j['Instrument'])
                    Instr = j['Instrument'].unique()
                    j['Ask'] = j['Ask'].apply(np.float)
                    j['Bid'] = j['Bid'].apply(np.float)
                    j['Spread'] = (j['Ask']-j['Bid']) 
                    j = j.set_index('Time')
                    for inst in Instr:
                        j[j.Instrument.isin([inst])].to_hdf(store, inst, append=True)
                                                                                   


