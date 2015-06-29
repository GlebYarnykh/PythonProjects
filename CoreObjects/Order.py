__author__ = 'ruayhg'
from Parsers.SdtUtils import get_sdt_side, client_names_parser, client_groups_parser
from Parsers.SdtUtils import get_sdt_deal_flags_representation
from Parsers.SdtUtils import root_path

import pandas as pd
import numpy as np

client_id_map_path = root_path + 'GlobalLogsToRead\\client_id_map.csv'
client_groups_map_path = root_path + 'GlobalLogsToRead\\client_group_map.csv'
client_id_map = client_names_parser(client_id_map_path)
client_groups_map = client_groups_parser(client_groups_map_path)


class Order(object):
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
        self.hedging_group = client_groups_map.loc[hedging_group, 'Name']
        self.client_name = client_id_map.loc[client_id, 'Name']
        self.flags = get_sdt_deal_flags_representation(flags)
        self.side = get_sdt_side(side)

if __name__ == "__main__":
    order = Order(9, 155, 100000, 51.515, 5, 1, "USD/RUB_TOM", 1, "HUY V ROT NA", 10000)
    print(order.hedging_group, order.client_comment_text)