__author__ = 'Gleb'

def initial_hedging_sizes_distribution(hedge_deals, req_lot):
    hedge_deals = hedge_deals.sort('Time', ascending=True)
    hedge_deals = hedge_deals.loc[hedge_deals['Source']!='RBRU']
    hedge_deals['sum'] = hedge_deals['Amount'].cumsum()
    hedge_deals = hedge_deals.loc[hedge_deals['sum']<=req_lot]
    hedge_deals['CompKey'] = hedge_deals['Source']+' '+ hedge_deals['Instrument']
    target = hedge_deals.groupby('CompKey').agg({'Amount':np.sum})
    initial_size = target['Amount'].sum()
    if 'MOEX USD/RUB_TOD' in target.index:
        moex_tod = target.at['MOEX USD/RUB_TOD', 'Amount']
    else:
        moex_tod = 0
    if 'MOEX USD/RUB_TOM' in target.index:
        moex_tom = target.at['MOEX USD/RUB_TOM', 'Amount']
    else:
        moex_tom = 0
    mm_deals = target.loc[(target.index!='MOEX USD/RUB_TOD') & (target.index!='MOEX USD/RUB_TOM'), 'Amount'].sum()
    return moex_tom/req_lot, moex_tod/req_lot, mm_deals/req_lot