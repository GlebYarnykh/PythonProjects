from datetime import datetime
import re
import numpy as np
from CoreObjects.ClientQuote import create_client_quote
from CoreObjects.OrderBook import OrderBook

__author__ = 'Gleb'


def fill_client_deal_part(row, deal):
    if ('selected group' in row) and (deal.hedging_group is np.nan):
        parse_hedging_group(deal, row)
        return np.nan
    # elif ('order_id: ' in row) and (deal.order_id is np.nan):
    #     return np.nan
    elif ('LocalOrders: SendOrderToSubscribers, action: 4' in row) and (str(deal.order_id) in row):
        parse_aggr_id_set_change_time(deal, row)
        return 'End of Deal'
    elif 'exist = ' in row:
        parse_best_side_quote(deal, row)
        return np.nan
    elif 'price_tolerance =' in row:
        parse_tolerance(deal, row)
        return np.nan
    elif ('current book, entries count' in row):
        parse_initial_book_capasity(deal, row)
        return np.nan
    elif ('exist = ' not in row) and ('FXBA::V2::BOOK2::ENTRY' in row):
        parse_book_quote(deal, row)
        return np.nan
    elif ('validate price' in row):
        parse_do_not_validate(deal,row)
        return np.nan
    elif ('process no better prices' in row):
        parse_no_better_prices(deal, row)
        return np.nan
    elif ('LocalOrders: src deal: quote time (ms)' in row):
        parse_quote_time(deal, row)
        return np.nan
    elif ('LocalOrders: src deal: LocalOrdersTypes::DEAL::REQ: FXBA::V2::DEAL::REQ:' in row):
        parse_validation_price_and_order_id(deal, row)
        return np.nan
    elif ('LocalOrders: ---- FXBA::V2::ORDER::ENTRY:' in row):
        parse_end_of_deal(deal, row)
        return np.nan
    else:
        pass

def parse_initial_book_capasity(deal, row):
    fill_method_execution_time(deal, row, "Initial Book Capacity")
    entries = int(re.search('entries count: (.*)', row).group(1))
    deal.book_capacity = entries


def parse_validation_price_and_order_id(deal, row):
    fill_method_execution_time(deal, row, "Validate Price")
    deal.validation_price = float(re.search(' price = (.+?),', row).group(1))
    deal.order_id = int(re.search(' order_id = (.+?),', row).group(1))
    deal.aggr_id = int(re.search(' aggr_id = (.+?),', row).group(1))


def parse_end_of_deal(deal, row):
    fill_method_execution_time(deal, row, "End of Deal")
    executed_lot = float(re.search(' executed_lot = (.+?),', row).group(1))
    executed_price = float(re.search(' avg_exec_price = (.+?),', row).group(1))
    deal.executed_lot, deal.executed_price = executed_lot, executed_price


def parse_do_not_validate(deal, row):
    fill_method_execution_time(deal, row, "Do not validate price")
    deal.do_not_validate = True


def parse_best_side_quote(deal, row):
    fill_method_execution_time(deal, row, "Best Side")
    id = int(re.search('id = (.+?),', row).group(1))
    side = int(re.search('type = (.+?),', row).group(1))
    price = float(re.search(' price = (.+?),', row).group(1))
    instrument = deal.order.ccy_pair
    book_type = int(re.search('flags = (.+?),', row).group(1))
    size = float(re.search('volume = (.+?),', row).group(1))
    client_quote = create_client_quote(id, side, price, instrument, book_type, size)
    deal.client_book = OrderBook(book_type, "ClientBook")
    if client_quote.Side == 'Bid':
        deal.best_bid = client_quote
    elif client_quote.Side == 'Ask':
        deal.best_ask = client_quote
    else:
        pass


def parse_book_quote(deal, row):
    fill_method_execution_time(deal, row, "Get Quote")
    id = int(re.search('id = (.+?),', row).group(1))
    side = int(re.search('type = (.+?),', row).group(1))
    price = float(re.search(' price = (.+?),', row).group(1))
    instrument = deal.order.ccy_pair
    book_type = int(re.search('flags = (.+?),', row).group(1))
    size = float(re.search('volume = (.+?),', row).group(1))
    client_quote = create_client_quote(id, side, price, instrument, book_type, size)
    if (client_quote.Side == 'Ask') and (deal.best_ask is np.nan):
        deal.best_ask = client_quote
    elif (client_quote.Side == 'Bid') and (deal.best_bid is np.nan):
        deal.best_bid = client_quote
    if deal.client_book is np.nan:
        deal.client_book = OrderBook(book_type, "ClientBook")
    if deal.current_entries < deal.book_capacity:
        deal.client_book.add_quote(client_quote)
        deal.current_entries += 1


def parse_tolerance(deal, row):
    fill_method_execution_time(deal, row, "Get Tolerance")
    deal.price_tolerance = float(re.search('price_tolerance = (.*)', row).group(1))


def parse_no_better_prices(deal, row):
    fill_method_execution_time(deal, row, "No better prices")
    deal.no_better_prices = True


def parse_quote_time(deal, row):
    fill_method_execution_time(deal, row, "Quote Lifetime")
    deal.quote_lifetime = float(re.search('quote time \(ms\): (.*)', row).group(1))


def parse_aggr_id_set_change_time(deal, row):
    fill_method_execution_time(deal, row, 'Generate Order')
    set_time = int(re.search('set_time = (.+?),', row).group(1))
    change_time = int(re.search('change_time = (.+?),', row).group(1))
    exact_time = float(row.split('->')[0].split(' ')[2])
    deal.end_of_deal = exact_time
    deal.set_time, deal.change_time = set_time, change_time


def parse_order_id(deal, row):
    fill_method_execution_time(deal, row, 'Generating Order Id')
    order_id = int(re.search('order_id: (.*)', row).group(1))
    deal.order_id = order_id


def parse_hedging_group(deal, row):
    fill_method_execution_time(deal, row, 'Choosing Hedging Group')
    if deal.hedging_group is np.nan:
        group = int(re.search('selected group (.*)', row).group(1))
        deal.set_hedging_group(group)
        deal.hedging_group_id = group


def fill_method_execution_time(deal, row, type_name):
    exact_time = float(row.split('->')[0].split(' ')[2])
    deal.methods_time.add_sdt_method_execution_time(type_name, exact_time)


def parse_datetime(date):
    if '.' in date:
        datetime_date = datetime.strptime(date, '%Y/%m/%d %H:%M:%S.%f')
    else:
        datetime_date = datetime.strptime(date, '%Y/%m/%d %H:%M:%S')
    return datetime_date, datetime_date.year, datetime_date.month, datetime_date.day