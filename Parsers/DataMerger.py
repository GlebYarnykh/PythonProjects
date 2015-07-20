# -*- coding: utf-8 -*-
"""
Created on Wed Dec 10 10:23:49 2014

@author: ruayhg
"""

from glob import glob
import numpy as np
import pandas as pd
from dateutil import parser

def Data_merger(type, pattern):
    
    files_list = glob("F:\\DataBase\\" + type + "\\*" + pattern)

    Deals = pd.DataFrame()
    for i in files_list:
        data = pd.read_csv(i, sep=";", decimal = ",", na_values='-')
        if 'Time (Local)' in data.columns.tolist():
            data = data.rename(columns = {'Time (Local)': 'Time'})
        if 'CCY Pair' in data.columns.tolist():
            data = data.rename(columns = {'CCY Pair': 'Instrument'})
        if 'Time (GMT)' in data.columns.tolist():
            data = data.rename(columns = {'Time (GMT)': 'Time'})
        if 'Profit (USD)' in data.columns.tolist():
            data = data.rename(columns = {'Profit (USD)': 'Profit'})
        if 'Amount (USD)' in data.columns.tolist():
            data = data.rename(columns = {'Amount (USD)': 'Amount(USD)'})
        if 'Rate' in data.columns.tolist():
            data = data.rename(columns = {'Rate': 'Price'})
        if 'Soft PnL (USD)' in data.columns.tolist():
            data = data.rename(columns = {'Soft PnL (USD)': 'SoftPnL(USD)'})
        if 'Set Time (GMT)' in data.columns.tolist():
            data = data.rename(columns = {'Set Time (GMT)': 'Set Time'})
        if 'Change Time (GMT)' in data.columns.tolist():
            data = data.rename(columns = {'Change Time (GMT)': 'Change Time'})
        if 'Set Time (Local)' in data.columns.tolist():
            data = data.rename(columns = {'Set Time (Local)': 'Set Time'})
        if 'Change Time (Local)' in data.columns.tolist():
            data = data.rename(columns = {'Change Time (Local)': 'Change Time'})
        if 'Exec Lot' in data.columns.tolist():
            data = data.rename(columns = {'Exec Lot': 'ExecLot'})
        if 'Avg Exec Price' in data.columns.tolist():
            data = data.rename(columns = {'Avg Exec Price': 'AvgExecPrice'})
        if 'Client Side (Pair)' in data.columns.tolist():
            data = data.rename(columns = {'Client Side (Pair)': 'Side'})
        Deals = pd.concat([Deals,data])

    Deals['Aggr Id'] = Deals['Aggr Id'].apply(str)
    Deals['Aggr Id'] = Deals['Aggr Id'].apply(lambda x: x[:-4] if '-' in x else x)
    Deals['Aggr Id'] = Deals['Aggr Id'].apply(int)
    Deals['Connection Type'] = Deals['Trader'].apply(lambda x: x[-4:] if '.' in x else 'Manual')
    return Deals

def comission_array(Deals):

    ''' Calculate Deals Comissions '''

    # Client Deals comission
    '1.3$ per 1m USD'
    # LPs Deals comission
    '360t - 12$ per 1m USD'
    'Intergral 5.7$ per 1m USD'
    'Micex - 14$ per 1m USD'
    'EBS - 5.8$ per 1m USD'

    Deals = Deals[(Deals['Result']=='Ok')&(Deals['Flags']!='Swap')&(Deals['Flags']!='Swap Counter CCY')&(Deals['Flags']!='Deleted External')&(Deals['Flags']!='Deleted')&(Deals['Flags']!='Deleted Counter CCY')]

    Deals = Deals.reset_index()
    del Deals['index']

    # Comissions
    Deals['Comission_per_Deal'] = 0
    Deals['Comission_per_Deal'] = Deals.apply(lambda row: 0.8 if (row['Source']!='MCX' and row['Source']!='MOEX') else 0, axis=1)

    Deals['FX Aggr Comission'] = Deals.apply(lambda row: row['Amount(USD)']*2.75/1000000 if (row['Trader']=='DEALER' and row['Source']!='RBRU' and row['Source']!='RZBM' and row['Source']!='RZBM2') else 0, axis=1)
    Deals['Comission_Int'] = Deals.apply(lambda row: row['Amount(USD)']*5.7/1000000 if (row['Connection Type']=='.int') else 0, axis=1)
    Deals['Comission_360t'] = Deals.apply(lambda row: row['Amount(USD)']*12/1000000 if (row['Connection Type']=='360t') else 0, axis=1)
    Deals['Comission_MOEX'] = Deals.apply(lambda row: row['Amount(USD)']*14/1000000 if (row['Source']=='MOEX') else 0, axis=1)
    Deals['Comission_EBS'] = Deals.apply(lambda row: row['Amount(USD)']*20/1000000 if (row['Source']=='EBS' and row['Trader']=='DEALER') else 0, axis=1)

    Deals['Total Comission'] = Deals['Comission_per_Deal'] + Deals['FX Aggr Comission'] + Deals['Comission_Int'] + Deals['Comission_360t'] + Deals['Comission_MOEX'] + Deals['Comission_EBS']

    Deals['Aggr Id'] = Deals['Aggr Id'].apply(str)
    Deals['Aggr Id'] = Deals['Aggr Id'].apply(lambda x: x[:-4] if '-' in x else x)
    Deals['Aggr Id'] = Deals['Aggr Id'].apply(int)

    Aggr = Deals.groupby(['Aggr Id'], sort=False).agg({'Total Comission': np.sum})
    Aggr.fillna(0.0)
    return Aggr
