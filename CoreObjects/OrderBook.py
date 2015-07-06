__author__ = 'ruayhg'
import pandas as pd

client_fields = ['QuoteId', 'Instrument', 'QuotingType', 'Price', 'Side', 'Size']
provider_fields = []


def get_quote_fields(source_type):
    if source_type == "ClientBook":
        quote_fields = client_fields
    elif source_type == "ProvidersBook":
        quote_fields = provider_fields
    else:
        quote_fields = "Unknown OrderBook Type"
    return quote_fields

class OrderBookSide(object):
    def __init__(self, source_type, side):
        quote_fields = get_quote_fields(source_type)
        self.side = side
        self.quotes = pd.DataFrame(columns=quote_fields)

class BookSide(OrderBookSide):
    def __init__(self, source_type, side):
        super(source_type, side)

class BandSide(OrderBookSide):
    def __init__(self, source_type, side):
        super(source_type, side)


class OrderBook(object):
    def __init__(self, bands_book_type, source_type):
        if bands_book_type == 0:
            self.bid_side = BookSide(source_type, "Bid")
            self.ask_side = BookSide(source_type, "Ask")
        else:
            self.bid_side = BandSide(source_type, "Bid")
            self.ask_side = BandSide(source_type, "Ask")

    def add_quote(self, quote):
        if quote.Side == "Bid":
            self.bid_side.quotes = self.bid_side.quotes.append(quote)
            self.bid_side.quotes.sort(columns='Price', axis=0, ascending=True)
        elif quote.Side == "Ask":
            self.ask_side.quotes = self.ask_side.quotes.append(quote)
            self.ask_side.quotes.sort(columns='Price', axis=0, ascending=False)