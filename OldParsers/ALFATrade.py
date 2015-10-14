# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 15:50:58 2015

@author: Администратор
"""

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


def parse_ALFA_trade_session(logs_path, current_year, current_month, current_day):
    ''' ALFA FIX Trade session parser '''
    test2 = pd.read_csv(logs_path, header=None, encoding='utf-8', sep=';')

    def extract_params(x, year, month, day):
        if isinstance(x, float):
            return 'None', 'None', 'None', 'None', 'None', 'None', 'None'
        else:
            if 'OnCmdResp' in x:
                part = x.split('->')[0]
                first = part.split(' ')
                time = datetime.strptime(first[1], '%H:%M:%S.%f').replace(year=year, month=month, day=day)
                deal_type = 'IN'
                exec_price = float(re.findall('avg_price = (.+?),', x)[0])
                exec_vol = float(re.findall('exec_lot = (.+?),', x)[0])
                deal_id = re.findall('user_deal_id = (.+?),', x)[0]
                instrument = re.findall('instr_name = (.+?),', x)[0]
                side = re.findall('side = (.+?),', x)[0]
                return time, deal_type, deal_id, exec_price, exec_vol, instrument, side
            elif 'Deal: cl_id' in x:
                part = x.split('->')[0]
                first = part.split(' ')
                time = datetime.strptime(first[1], '%H:%M:%S.%f').replace(year=year, month=month, day=day)
                deal_type = 'OUT'
                req_price = float(re.findall('price = (.+?),', x)[0])
                req_vol = float(re.findall('lot = (.+?),', x)[0])
                deal_id = re.findall('user_deal_id = (.+?),', x)[0]
                instrument = re.findall('instr_name = (.+?),', x)[0]
                side = 'None'
                return time, deal_type, deal_id, req_price, req_vol, instrument, side
            else:
                return 'None', 'None', 'None', 'None', 'None', 'None', 'None'

    test2['Time'], test2['in_out'], test2['id'], test2['exec_price'], test2['exec_vol'], test2['instr'], test2[
        'side'] = np.vectorize(extract_params)(test2[0], current_year, current_month, current_day)

    out = test2[test2['in_out'] == 'OUT']
    if out.empty == False:
        out = out.set_index(out['id'])
        ins = test2[test2['in_out'] == 'IN']
        ins = ins.set_index(ins['id'])
        order_time = {}
        for i in set(out.id.tolist()):
            times = [out.at[i, 'Time'], out.at[i, 'instr'], float(out.at[i, 'exec_vol']),
                     float(out.at[i, 'exec_price'])]

            deal = ins[ins['id'] == i]
            times.append(deal['Time'].max())
            times.append(float(deal['exec_price'].mean()))
            times.append(float(deal['exec_vol'].max()))
            times.append(deal.at[i, 'side'])
            order_time[i] = times

        array = pd.DataFrame(order_time).T
        array.columns = ['Time', 'Instrument', 'req_vol', 'req_price', 'exec_time', 'exec_price', 'exec_vol',
                         'side']
        array['exec_time'] = array['exec_time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
        array['Time'] = array['Time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))

        def price_improvement(req_p, exec_p, side):
            if side == '1' and exec_p != 0:
                imp = exec_p - req_p
            elif side == '0' and exec_p != 0:
                imp = req_p - exec_p
            else:
                imp = 0
            return imp

        array['improvement'] = np.vectorize(price_improvement)(array['req_price'], array['exec_price'],
                                                               array['side'])
        array['full_time_diff'] = (array['exec_time'] - array['Time']).apply(
            lambda x: (x / np.timedelta64(1, 's')) * 1000)
        array['exec_%'] = array['exec_vol'] / array['req_vol']
        array['LP'] = 'ALFA'

        return array, ins[['Time']]
    else:
        del out[0], out['in_out']
        return out, out


def generate_ALFA_execution_report(output_name, start_date, end_date):
    ''' Parsing '''
    date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days)]

    Deals = pd.DataFrame()
    Special_rejects = pd.DataFrame()
    for i in date_generated:
        date = i.strftime("%Y.%m.%d")
        current_year = int(date[:4])
        current_month = int(date[5:7])
        current_day = int(date[8:10])
        logs_path = 'L:\\Alfa\\' + date + '\\fxba_net_srv.log'
        if logs_path in glob('L:\\Alfa\\' + date + '\\*'):
            new_data, unusual_rejects = parse_ALFA_trade_session(logs_path, current_year, current_month, current_day)
            Deals = pd.concat([Deals, new_data])
            Special_rejects = pd.concat([Special_rejects, unusual_rejects])
    Rejects_path = 'D:\\LPs execution\\CMRZ\\rejects.csv'
    Special_rejects.loc[Special_rejects['status'] == '8'].to_csv(Rejects_path)

    matplotlib.rcParams['figure.figsize'] = 15, 12
    pp = PdfPages(output_name)
    for i in set(Deals['Instrument'].tolist()):
        Instr = Deals.loc[Deals['Instrument'] == i]
        if len(Instr) > 1:
            fig, axes = plt.subplots(2, 2)

            Instr['full_time_diff'].hist(bins=25, ax=axes[1, 0])
            Instr.loc[Instr['exec_vol'] != 0, 'improvement'].hist(bins=20, ax=axes[1, 1])
            Instr['exec_%'].hist(bins=10, ax=axes[0, 0])
            Instr['req_vol'].hist(bins=20, ax=axes[0, 1])
            axes[0, 0].set_title('% of executed amount on each deal')
            axes[0, 1].set_title('Requested amount on each deal')
            axes[1, 0].set_title('Time of execution')
            axes[1, 1].set_title('Price improvement')
            fig.tight_layout()
            pl.suptitle(i)
            pp.savefig(fig)
            fig.clf()

    pp.close()
