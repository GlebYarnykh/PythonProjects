# -*- coding: utf-8 -*-
"""
Created on Mon Nov 17 16:47:59 2014

@author: ruayhg
"""

import pandas as pd
from dateutil import parser
from pandas.stats.moments import rolling_mean



def queue_parser(queuepath, current_year, current_month, current_day):

    queue = pd.read_csv(queuepath, sep=' -> ', names = ['TimeProx', 'Queue'])
    
    queue['Time'] = queue['TimeProx'].apply(lambda x: parser.parse(x.split(' ', 3)[1]).replace(year=current_year, month=current_month, day=current_day))
    queue['WrongQueue'] = queue['Queue'].apply(lambda x: True if ':' in x else False)
    
    queue = queue[queue['WrongQueue']==False]
    queue['Queue'] = queue['Queue'].apply(lambda x: int(x.split(' = ', 2)[1]))
    del queue['TimeProx']
    del queue['WrongQueue']
    queue = queue.set_index(queue['Time'])
    queue['Queue(ms)'] = (queue['Queue']/30)*2.5
    
    return queue



