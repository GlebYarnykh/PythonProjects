import re

__author__ = 'Gleb'


def parse_ioc_index(row, index):
    if "LocalOrders: Deal, LocalOrdersTypes" in row:
        return index
    else:
        return 0


def parse_limit_index(row):
    if 'LocalOrders: order ready to exec' in row:
        order_id = int(re.search('order_id = (.*)', row).group(1))
        return order_id
    else:
        return 0


def parse_marination_index(row, index):
    if ("Deal: FXBA::V2::DEAL::REQ:" in row) and ("Try" not in row):
        return index
    else:
        return 0

