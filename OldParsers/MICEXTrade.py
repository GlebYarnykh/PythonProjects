
import pandas as pd
import numpy as np
from dateutil import parser
from pandas.stats.moments import rolling_mean
import gc
import re
from datetime import datetime
from datetime import timedelta
import pylab as pl
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from glob import glob

def parse_MICEX_trade_session(logs_path, current_year, current_month, current_day):
    ''' MICEX FIX Trade session parser '''
    test2 = pd.read_csv(logs_path, header = None, encoding='utf-8', sep=';')
    
    def extract_time_and_type(x, year, month, day):
        for_time = re.findall(' (.+?) ->', x)[0].split(' ')
        time = datetime.strptime(for_time[0], '%H:%M:%S.%f').replace(year=year, month=month, day=day)
        in_out = re.findall(' (.+?) ', x)[2]       
        return time, in_out    
    
    def extract_params(x, year, month, day):
        part = x.split(': 8')[0]
        first = part.split(' ')
        time = datetime.strptime(first[1], '%H:%M:%S.%f').replace(year=year, month=month, day=day)
        mes_type = re.findall('35=.', x)[0]
        if (mes_type == '35=0') or (mes_type == '35=1'):
            in_out = 'Heart'
        elif mes_type == '35=D':
            in_out = 'OUT'
        elif mes_type == '35=8':
            in_out = 'IN'
        else:
            in_out = None
        
        return time, in_out
    
    test2['Time'], test2['in_out'] = np.vectorize(extract_params)(test2[0], current_year, current_month, current_day)
    
    def get_in_order_params(x):        
        order_id = re.findall('11=(.+?)'+chr(1), x)[0]
        order_status = re.findall('39=(.+?)'+chr(1), x)[0]
        instr = re.findall('55=(.+?)'+chr(1), x)[0]
        if len(re.findall('58=(.+?)'+chr(1), x))>0:
            Reject_reason1 = re.findall('58=(.+?)'+chr(1), x)[0]
        else:
            Reject_reason1 = None
        if len(re.findall('103=(.+?)'+chr(1), x))>0:
            Reject_reason2 = re.findall('103=(.+?)'+chr(1), x)[0]
        else:
            Reject_reason2 = None
        return order_id, order_status, instr, Reject_reason1, Reject_reason2
    
    def get_out_order_params(x):
        
        order_id = re.findall('11=(.+?)'+chr(1), x)[0]
        instr = re.findall('55=(.+?)'+chr(1), x)[0]
        requested_vol = np.float(re.findall('38=(.+?)'+chr(1), x)[0])
        
        if len(re.findall('44=(.+?)'+chr(1), x))>0:
            requested_price = np.float(re.findall('44=(.+?)'+chr(1), x)[0])
        else:
            requested_price = None
        
        side = re.findall('54=(.+?)'+chr(1), x)[0]
        if len(re.findall('117=(.+?)'+chr(1), x))>0:
            QuoteId = re.findall('117=(.+?)'+chr(1), x)[0]
        else:
            QuoteId = None
        
        return order_id, instr, requested_price, requested_vol, side, QuoteId
    
    def get_exec_params(x):
        if len(re.findall('31=(.+?)'+chr(1), x))>0:
            exec_price = re.findall('31=(.+?)'+chr(1), x)[0]
        else:
            exec_price = np.nan;
        exec_vol = re.findall('14=(.+?)'+chr(1), x)
        
        if len(exec_vol)>0:
            exec_vol = np.float(exec_vol[0])
        else:
            exec_vol = 0
        return exec_price, exec_vol
        
    out = test2[test2.in_out.isin(['OUT'])]
    if out.empty==False:
        out['id'], out['instr'], out['req_p'], out['req_v'], out['side'], out['QuoteId'] = np.vectorize(get_out_order_params)(out[0])
        out = out.set_index(out['id'])
        
        ins = test2[test2.in_out.isin(['IN'])]
        ins['id'], ins['status'], ins['instr'], ins['Reject1'], ins['Reject2'] = np.vectorize(get_in_order_params)(ins[0])
        ins['exec_p'], ins['exec_v'] = np.vectorize(get_exec_params)(ins[0])
        ins = ins.set_index(ins['id'])
        
        order_time = {}
        for i in set(out.id.tolist()):
            times = []
            times.append(out.at[i, 'Time'])
            times.append(out.at[i, 'instr'])
            times.append(out.at[i, 'side'])
            times.append(out.at[i, 'req_v'])
            times.append(out.at[i, 'req_p'])
            times.append(out.at[i, 'QuoteId'])
            
            deal = ins[ins.id.isin([i])]
            if deal[deal.status.isin(['1','2'])].empty == False:
                times.append(deal[deal.status.isin(['1','2'])]['Time'].max())
                times.append(deal[deal.status.isin(['1','2'])]['exec_p'].mean())
            else:
                times.append(np.nan)  
                times.append(np.nan)
            times.append(deal['Time'].max())
            times.append(deal['exec_v'].max())          
            
            order_time[i] = times    
        
        Time_array = pd.DataFrame(order_time).T
        Time_array.columns = ['Time', 'Instrument', 'side', 'req_vol', 'req_price', 'QuoteId', 'exec_time', 'exec_price', 'end_time', 'exec_vol']
        
        def price_improvement(req_p, exec_p, side):
            if side=='2' and req_p!=None:
                imp = exec_p - req_p
            elif side =='1' and req_p!=None:
                imp = req_p - exec_p
            else:
                imp = 0
            return imp
            
        Time_array['improvement'] = (Time_array['req_price'] - Time_array['exec_price']).apply(np.abs)
        Time_array['full_time_diff'] = (Time_array['end_time'] - Time_array['Time']).apply(lambda x: (x/np.timedelta64(1,'s'))*1000)
        Time_array['exec_%'] = Time_array['exec_vol']/Time_array['req_vol']
        Time_array['LP']='MICEX'
        
        return Time_array, ins[['Time', 'status', 'Reject1', 'Reject2']]
    else:
        del out[0], out['in_out']
        return out, out        
    

def generate_MICEX_execution_report(output_name, start_date, end_date):
    ''' Parsing '''
    date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]
    
    Deals = pd.DataFrame()
    Special_rejects = pd.DataFrame()
    for i in date_generated:
        date = i.strftime("%Y.%m.%d")
        current_year = int(date[:4])
        current_month = int(date[5:7])
        current_day = int(date[8:10])
        logs_path = 'K:\\logs\\micex3\\'+date+'\\micex3_MD0054300383-GWMFIX1CUR_trade_messages.log'
        if logs_path in glob('K:\\logs\\micex3\\'+date+'\\*'):           
            new_data, unusual_rejects = parse_MICEX_trade_session(logs_path, current_year, current_month, current_day)
            Deals = pd.concat([Deals, new_data])
            Special_rejects = pd.concat([Special_rejects, unusual_rejects]) 
    Rejects_path = 'D:\\LPs execution\\CMRZ\\rejects.csv'
    Special_rejects.loc[Special_rejects['status']=='8'].to_csv(Rejects_path) 
           
    matplotlib.rcParams['figure.figsize'] = 15,12
    pp = PdfPages(output_name)
    for i in set(Deals['Instrument'].tolist()):
        Instr = Deals.loc[Deals['Instrument']==i]
        if len(Instr)>1:

            fig, axes = plt.subplots(2, 2)    
            
            Instr['full_time_diff'].hist(bins=25, ax=axes[1,0])
            Instr['improvement'].hist(bins=20, ax=axes[1,1])
            Instr['exec_%'].hist(bins=10, ax=axes[0,0])
            Instr['req_vol'].hist(bins=20, ax=axes[0,1])
            axes[0,0].set_title('% of executed amount on each deal')
            axes[0,1].set_title('Requested amount on each deal')
            axes[1,0].set_title('Time of execution')
            axes[1,1].set_title('Price improvement')
            fig.tight_layout()
            pl.suptitle(i)
            pp.savefig(fig)
            fig.clf()
    
    pp.close()

    
