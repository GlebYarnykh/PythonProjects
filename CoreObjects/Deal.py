from Parsers.SdtUtils import get_sdt_side, client_names_parser
from Parsers.SdtUtils import get_sdt_flags_representation
__author__ = 'Администратор'
import pandas as pd
import numpy as np

client_id_map_path = ""
client_id_map = client_names_parser(client_id_map_path)

class Deal(object):
    def __init__(self, client_id, user_deal_id, requested_lot, requested_price, hedging_group,
                 side, ccy_pair, flags, comment_text, minimum_lot):
        # Initial part from first "Deal" entree
        self.client_id = client_id
        self.user_deal_id = user_deal_id
        self.requested_lot = requested_lot
        self.minimum_lot = minimum_lot
        self.requested_price = requested_price
        self.ccy_pair = ccy_pair
        self.client_comment_text = comment_text
        self.hedging_group = client_groups_map.loc[hedging_group, ]
        self.client_name = client_id_map.loc[client_id, 'client_name']
        self.flags = get_sdt_flags_representation(flags)
        self.side = get_sdt_side(side)
        # Filled on each line
        self.methods_execution_time = pd.DataFrame()
        # Filled from client_trade logs
        self.order_id = None
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
        




