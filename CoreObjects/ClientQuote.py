from CoreObjects.OrderBook import client_fields

__author__ = 'ruayhg'

import pandas as pd

def create_client_quote(quote_id, side, price, instrument, book_type, size):
    if side == 1:
        string_side = "Ask"
    else:
        string_side = "Bid"
    if book_type == 1:
        quoting_type = "Book"
    else:
        quoting_type = "Bands"
    client_quote = pd.Series([quote_id, instrument, quoting_type, price, string_side, size],
                     index = client_fields,
                     name = quote_id)
    return client_quote
