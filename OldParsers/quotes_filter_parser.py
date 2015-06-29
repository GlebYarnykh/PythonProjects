# -*- coding: utf-8 -*-
"""
Created on Wed Nov 19 16:45:20 2014

@author: ruayhg
"""

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

def take_subset(pandas_array, startDate, endDate):
    return pandas_array[(pandas_array['Time']>startDate)&(pandas_array['Time']<endDate)]
    

def quotes_filter_parser_deletedonly(queuepath, current_year, current_month, current_day):
    
    
    instr = pd.read_csv(queuepath, sep=' -> ', names = ['TimeProx', 'Instr'])
    instr = instr.dropna()
    instr['Time'] = instr['TimeProx'].apply(lambda x: parser.parse(x.split(' ', 3)[1]).replace(year=current_year, month=current_month, day=current_day))
    instr['Action'] = instr['Instr'].apply(lambda x: x.split(', ')[0])    
    
    instr = instr[instr['Action']=='  Entry deleted']
    instr['Source'] = instr['Instr'].apply(lambda x: x.split(', ')[1].split(': ')[1])   
    instr['CCY Pair'] = instr['Instr'].apply(lambda x: x.split(', ')[2])
    instr['Id'] = instr['Instr'].apply(lambda x: x.split(', ')[3].split(': ')[1])
    instr['QuoteAction'] = instr['Instr'].apply(lambda x: np.float(x.split(', ')[5].split(': ')[1]))
    instr['Side'] = instr['Instr'].apply(lambda x: x.split(', ')[6])
    instr['Price'] = instr['Instr'].apply(lambda x: np.float(x.split(', ')[7].split(': ')[1])) 
    instr['Band'] = instr['Instr'].apply(lambda x: np.float(x.split(', ')[8].split(': ')[1])/(1000000)) 
    
    instr['For Count'] = 1
    
    del instr['TimeProx']
    del instr['Instr']
    del instr['Action']
    
    return instr
    
    
def quotes_filter_parser_filterdone(queuepath, current_year, current_month, current_day):
     
    instr = pd.read_csv(queuepath, sep=' -> ', names = ['TimeProx', 'Instr'])
    instr = instr.dropna()
    instr['Time'] = instr['TimeProx'].apply(lambda x: parser.parse(x.split(' ', 3)[1]).replace(year=current_year, month=current_month, day=current_day))
    instr['Action'] = instr['Instr'].apply(lambda x: x.split(', ')[0])    
    
    instr = instr[instr['Action']=='  Filter action done']
    instr['CCY Pair'] = instr['Instr'].apply(lambda x: x.split(', ')[1].split(': ')[1])
    
    instr['Prices Before'] = instr['Instr'].apply(lambda x: x.split(', ')[2].split(': ')[1])
    
    instr['Before LP bid'] = instr['Prices Before'].apply(lambda x: x.split('/')[0].split('(')[1][:-1])
    instr['Before Price bid'] = instr['Prices Before'].apply(lambda x: np.float(x.split('/')[0].split('(')[0]))    
    instr['Before LP ask'] = instr['Prices Before'].apply(lambda x: x.split('/')[1].split('(')[1][:-1])
    instr['Before Price ask'] = instr['Prices Before'].apply(lambda x: np.float(x.split('/')[1].split('(')[0]))
    
    instr['Prices After'] = instr['Instr'].apply(lambda x: x.split(', ')[3].split(': ')[1])
    
    instr['After LP bid'] = instr['Prices After'].apply(lambda x: x.split('/')[0].split('(')[1][:-1])
    instr['After Price bid'] = instr['Prices After'].apply(lambda x: np.float(x.split('/')[0].split('(')[0]))    
    instr['After LP ask'] = instr['Prices After'].apply(lambda x: x.split('/')[1].split('(')[1][:-1])
    instr['After Price ask'] = instr['Prices After'].apply(lambda x: np.float(x.split('/')[1].split('(')[0]))
    
    
    del instr['TimeProx']
    del instr['Instr']
    del instr['Action']
    del instr['Prices Before']
    del instr['Prices After']
    
    return instr
    
''' Simple DELETED INSTR Analysis '''
def filter_analysis_by_day(date):
    
    current_year = int(date[:4])
    current_month = int(date[5:7])
    current_day = int(date[8:10])
    logs_path = 'M:\\logs\\aggr_data\\'+date+'\\_quote_checker.log'
    output_path = 'C:\\Raiffeisen_reports\\' + date[:4] +'\\' + date[5:7] +'\\'
    queuepath = logs_path
    
    deleted = quotes_filter_parser_deletedonly(logs_path, current_year, current_month, current_day)
    deleted['For Count'] = 1
    
    for_Time = deleted.groupby(['Time'],sort=False)
    TimeDiff = for_Time.agg({'For Count':np.sum})
    TimeDiff['Time'] = TimeDiff.index
    TimeDiff['For Count'] = 1
    deleted = deleted.set_index('Time')
    
    TimeDiff['TimeDiff'] = (TimeDiff['Time']-TimeDiff['Time'].shift()).fillna(0)
    TimeDiff['TimeDiff'] = TimeDiff['TimeDiff'].apply(lambda x: x/np.timedelta64(1, 'ms')).astype('int64')
    TimeDiff['Hour'] = TimeDiff['Time'].apply(lambda x: x.hour)
    
    for_LPs = deleted.groupby(['Source'],sort=False)
    deletedAmount_byLP = for_LPs.agg({'For Count':np.sum})
    deletedAmount_byLP = deletedAmount_byLP.sort('For Count',axis=0,ascending=False)
    
    for_hours = TimeDiff.groupby(['Hour'],sort=False)
    HourBar = for_hours.agg({'For Count': np.sum})
    
    for_CCY = deleted.groupby(['CCY Pair'],sort=False)
    deletedAmount_byCCY = for_CCY.agg({'For Count':np.sum})
    
    deleted['Time'] = deleted.index
    for_LPs_count = deleted.groupby(['Time','Source'],sort=False)
    deletedAmount_byLP_count = for_LPs_count.agg({'For Count':np.sum})
    
    LPs = pd.DataFrame()
    for i in list(set(deleted['Source'].tolist())):
        total = deletedAmount_byLP.loc[i,'For Count']
        count = deletedAmount_byLP_count.xs((i),level='Source')
        total_cases = len(count['For Count'].index)
        one_count = len(count[count['For Count']<=1].index)
        two_count = len(count[count['For Count']==2].index)
        three_count = len(count[count['For Count']>=3].index)
        
        vector = pd.Series([total, total_cases, one_count, two_count, three_count], index=['Total','Total Cases','One Deleted', 'Two Deleted','Three and more deleted'])
        vector.name = i
        LPs = LPs.append(vector)    
    LPs = LPs.sort('Total',axis=0,ascending=False)
    
    LPs['One and less %'] = LPs['One Deleted']/LPs['Total Cases']
    LPs['Two %'] = LPs['Two Deleted']/LPs['Total Cases']
    LPs['Three and more %'] = LPs['Three and more deleted']/LPs['Total Cases']
         
    ''' Equal LP analysis '''
    
    deleted_filterdone = quotes_filter_parser_filterdone(queuepath, current_year, current_month, current_day)
    deleted_filterdone['If Equal'] = deleted_filterdone.apply(lambda row: 1 if (row['Before LP bid']==row['Before LP ask']) else 0,axis=1)
    deleted_filterdone['Spread'] = (deleted_filterdone['Before Price bid']-deleted_filterdone['Before Price ask'])
    deleted_filterdone['Spread'] = deleted_filterdone.apply(lambda row: int(row['Spread']/0.001) if ('JPY' in row['CCY Pair'] or 'RUB' in row['CCY Pair']) else int(row['Spread']/0.00001),axis=1)
    for_equal = deleted_filterdone.groupby(['Before LP bid'],sort=False)
    Equal_bidask = for_equal.agg({'If Equal':np.sum})
        

    
    ''' Plotting '''
    pp = PdfPages(output_path+str(current_day)+'_Filter_report.pdf')
    matplotlib.rcParams['figure.figsize'] = 15,10
    
    ''' Chart 1 - what LPs are filtered '''
    fig, axes = plt.subplots(1,1)  
    deletedAmount_byLP.plot(kind='bar',ax=axes)
    pl.suptitle('Quotes filtered by LP')
    pp.savefig(fig)
    fig.clf()
    
    ''' Chart 2 - what CCY are filtered '''
    fig, axes = plt.subplots(1,1)
    deletedAmount_byCCY[deletedAmount_byCCY['For Count']>0].sort('For Count',ascending=False).plot(kind='bar',ax=axes)
    pl.suptitle('Quotes filtered by CCY')
    plt.tight_layout()
    pp.savefig(fig)
    fig.clf()
    
    ''' Chart 3 - Count by hour '''
    fig, axes = plt.subplots(1,1) 
    HourBar.plot(kind='bar',ax=axes)
    pl.suptitle('Quotes filtered by hour (local)')
    pp.savefig(fig)
    fig.clf()
      
    ''' Chart 7 - Top Pairs '''
    
    Pairs = ['EUR/USD','USD/RUB_TOM','USD/RUB_TOD','GBP/USD','USD/CAD','AUD/USD', 'USD/CHF', 'EUR/RUB_TOM', 'EUR/RUB_TOD',
             'EUR/GBP','USD/JPY','EUR/CAD', 'EUR/AUD', 'GBP/AUD', 'EUR/CHF', 'NZD/USD','EUR/USD_TOM','EUR/USD_TOD',
             'GBP/RUB_TOM', 'GBP/RUB_TOD']
    
    for i in Pairs:
        ''' Bins - [0, 1, 2, from 2 to 5, from 5 to 10, >10]'''
        pair = deleted_filterdone.loc[deleted_filterdone['CCY Pair']==i,:]
        pair['For Count']=1
        Grouped = pair.groupby(['Before LP bid', 'Before LP ask', 'Spread']).agg({'For Count':np.sum})
        Grouped = Grouped.reset_index()
        Grouped['Spread'] = Grouped['Spread'].apply(lambda x: int(str(x)))
        Sum = {}
        for j in set(Grouped['Before LP bid'].tolist()+Grouped['Before LP ask'].tolist()):
            dat = Grouped.loc[(Grouped['Before LP bid']==j)| (Grouped['Before LP ask']==j),:].groupby('Spread').agg({'For Count':np.sum}).reset_index()      
            Sum[j] = [dat.loc[dat['Spread']==0,'For Count'].sum(),dat.loc[dat['Spread']==1,'For Count'].sum(),
                      dat.loc[dat['Spread']==2,'For Count'].sum(),dat.loc[(dat['Spread']>2)&(dat['Spread']<=5),'For Count'].sum(),
                      dat.loc[(dat['Spread']>5)&(dat['Spread']<=10),'For Count'].sum(),dat.loc[(dat['Spread']>10),'For Count'].sum()]
        data = pd.DataFrame(Sum)
        if len(data)>0:
            fig, axes = plt.subplots(2,1) 
            my_colors = ['b','g','r','c','m','y','k','w', '#9400D3','#A52A2A','#B22222','b','g','r','c','m']
            my_colors = my_colors[:len(data.columns.tolist())]
            
            data.index = ['0','1','2','3-5','5-10','>10']
            data.plot(kind='barh',stacked=True,color=my_colors,ax=axes[0])
            data.T.plot(kind='barh',stacked=True,color=my_colors,ax=axes[1])
            pl.suptitle(i+' filter Cases')
            pp.savefig(fig)
            fig.clf()
               
    ''' Chart 5 - how much of LPs bands are filtered '''
    fig, axes = plt.subplots(1,1)
    LPs[['One and less %','Two %','Three and more %']].sort('One and less %').plot(kind='bar',ax=axes)
    pl.suptitle('One % - filtered <=1 LPs band per case \n Two % - filtered 2 LPs band per case \n Three and more % - filtered >=3 LPs band per case')
    pp.savefig(fig)
    fig.clf()
    
    ''' Chart 8 - Equal bid/ask from LP '''
    fig, axes = plt.subplots(1,1)
    Equal_bidask[Equal_bidask['If Equal']>10].sort('If Equal',ascending=False).plot(kind='bar',ax=axes)
    pl.suptitle('# Cases - best bid/ask from one LP has negative spread')
    pp.savefig(fig)
    fig.clf()
    pp.close()    


