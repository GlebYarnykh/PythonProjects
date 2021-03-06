from CoreObjects.MethodsExecutionTime import MethodsExecutionTime
from Parsers.SdtUtils import get_sdt_side, client_names_parser, client_groups_parser, root_path
from Parsers.SdtUtils import get_sdt_deal_flags_representation
import pandas as pd
import numpy as np

client_groups_map_path = root_path + 'client_group_map.csv'
client_groups_map = client_groups_parser(client_groups_map_path)


class Deal(object):
    def __init__(self, order, marination_time, unexpected_marination_time):
        # Initial part from first "Deal" entree - Order Class
        self.order = order
        # Filled on each line
        self.methods_time = MethodsExecutionTime(order.exact_time)
        # Filled from mc_mngr (marination + problems)
        self.marination_time = marination_time
        self.unexpected_marination_time = unexpected_marination_time
        # Filled from client_trade logs
        self.hedging_group_id = np.nan
        self.hedging_group = np.nan
        self.order_id = np.nan
        self.aggr_id = np.nan
        self.set_time = np.nan
        self.change_time = np.nan
        self.best_bid = np.nan
        self.best_ask = np.nan
        self.price_tolerance = np.nan
        self.client_book = np.nan
        self.book_capacity = 0
        self.current_entries = 0
        self.do_not_validate = False
        self.no_better_prices = False
        self.quote_lifetime = np.nan
        self.validation_price = np.nan
        self.executed_lot = np.nan
        self.executed_price = np.nan
        self.end_of_deal = np.nan
        self.pnl = np.nan
        self.soft_pnl = np.nan
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
        if self.best_ask is np.nan:
            best_ask = np.nan
        else:
            best_ask = self.best_ask.Price
        if self.best_bid is np.nan:
            best_bid = np.nan
        else:
            best_bid = self.best_bid.Price

        # TODO Create set_change_times and difference (or difference only)
        data = [self.order.order_type, self.order.exact_time, self.order.client_name, self.order.side, self.aggr_id,
                self.order_id, self.order.ccy_pair,
                self.order.requested_lot, self.order.requested_price, self.order.minimum_lot, self.order.flags,
                self.methods_time.methods_execution_time, self.hedging_group, self.hedging_group_id, best_ask, best_bid,
                self.price_tolerance, self.client_book.bid_side.quotes, self.client_book.ask_side.quotes,
                self.do_not_validate, self.no_better_prices, self.quote_lifetime, self.validation_price,
                self.executed_lot, self.executed_price, self.set_time, self.change_time, self.marination_time,
                self.unexpected_marination_time, self.end_of_deal]
        index = ['OrderType', 'ExactTime', 'ClientName', 'Side', 'AggrId', 'OrderId', 'Instrument', 'ReqLot', 'ReqPrice', 'MinLot', 'Flag',
                 'SdtMethodsExecution', 'HedgingGroup', 'GroupId', 'BestAsk', 'BestBid', 'Tolerance', 'BidQuotes',
                 'AskQuotes', 'DoNotValidate', 'NoBetterPrices', 'QuoteLifetime', 'ValPrice', 'ExecLot', 'ExecPrice',
                 'SetTime', 'ChangeTime', 'Marination', 'UnexpectedMarination', 'EndOfDeal']

        return pd.Series(data=data, index=index)
