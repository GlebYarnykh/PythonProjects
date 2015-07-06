from CoreObjects.MethodsExecutionTime import MethodsExecutionTime
from Parsers.SdtUtils import get_sdt_side, client_names_parser, client_groups_parser, root_path
from Parsers.SdtUtils import get_sdt_deal_flags_representation
import pandas as pd
import numpy as np

client_groups_map_path = root_path + 'client_group_map.csv'
client_groups_map = client_groups_parser(client_groups_map_path)


class Deal(object):
    def __init__(self, order):
        # Initial part from first "Deal" entree - Order Class
        self.order = order
        # Filled on each line
        self.methods_time = MethodsExecutionTime(order.exact_time)
        # Filled from client_trade logs
        self.hedging_group = np.nan
        self.order_id = np.nan
        self.aggr_id = np.nan
        self.set_time = np.nan
        self.change_time = np.nan
        self.best_bid = np.nan
        self.best_ask = np.nan
        self.price_tolerance = np.nan
        self.client_book = np.nan
        self.do_not_validate = False
        self.no_better_prices = False
        self.quote_lifetime = np.nan
        self.validation_price = np.nan
        self.executed_lot = np.nan
        self.executed_price = np.nan
        # Filled from aggr_trade logs
        self.trade_comment_text = np.nan
        self.providers_order_books = pd.Panel()
        self.hedging_deals = pd.DataFrame()
        # Filled from group_pricing logs
        self.price_path = pd.DataFrame()
        # Filled from quotes_filter
        self.filtered_quotes = pd.DataFrame()
        # Filled from quotes_queue
        self.queue = pd.DataFrame()

    def set_hedging_group(self, hedge_group_int):
        self.hedging_group = client_groups_map.loc[hedge_group_int, 'Name']

    def to_pandas_series_client_data_only(self):
        data = [self.order.exact_time, self.order.client_name, self.order.side, self.aggr_id, self.order_id,
                self.order.ccy_pair,
                self.order.requested_lot, self.order.requested_price, self.order.minimum_lot, self.order.flags,
                self.methods_time.methods_execution_time, self.hedging_group, self.best_ask.Price, self.best_bid.Price,
                self.price_tolerance, self.client_book.bid_side.quotes, self.client_book.ask_side.quotes,
                self.do_not_validate, self.no_better_prices, self.quote_lifetime, self.validation_price,
                self.executed_lot, self.executed_price]
        index = ['ExactTime', 'ClientName', 'Side', 'AggrId', 'OrderId', 'Instrument', 'ReqLot', 'ReqPrice', 'MinLot', 'Flag',
                 'SdtMethodsExecution', 'HedgingGroup', 'BestAsk', 'BestBid', 'Tolerance', 'BidQuotes',
                 'AskQuotes', 'DoNotValidate', 'NoBetterPrices', 'QuoteLifetime', 'ValPrice', 'ExecLot', 'ExecPrice']

        return pd.Series(data=data, index=index)
