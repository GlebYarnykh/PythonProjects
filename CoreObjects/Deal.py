from Parsers.SdtUtils import get_sdt_side, client_names_parser, client_groups_parser, root_path
from Parsers.SdtUtils import get_sdt_deal_flags_representation
import pandas as pd
import numpy as np

client_groups_map_path = root_path + 'GlobalLogsToRead\\client_group_map.csv'
client_groups_map = client_groups_parser(client_groups_map_path)

class Deal(object):
    def __init__(self, order):
        # Initial part from first "Deal" entree - Order Class
        self.order = order
        # Filled on each line
        self.methods_execution_time = pd.DataFrame()
        # Filled from client_trade logs
        self.order_id = None
        self.hedging_group = None
        self.aggr_id = None
        self.set_time = None
        self.change_time = None
        self.best_bid = None
        self.best_ask = None
        self.price_tolerance = None
        self.client_bids = pd.DataFrame()
        self.client_asks = pd.DataFrame()
        self.do_not_validate = False
        self.no_better_prices = False
        self.executed_lot = None
        self.executed_price = None
        # Filled from aggr_trade logs
        self.trade_comment_text = None
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





