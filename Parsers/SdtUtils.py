import pandas as pd

__author__ = 'Администратор'

def get_sdt_side(side_int):
    if side_int == 0:
        return "Buy"
    else:
        return "Sell"

def get_sdt_flags_representation(flag_int):
    return None

def get_hedging_group(hedging_group):
    pass

def client_names_parser(path):
    id_name_map = pd.read_csv(path)
    included_columns = ["Id", "Name"]
    id_name_map = id_name_map.loc[:, included_columns]
    id_name_map.set_index("Id", inplace=True)
    return id_name_map

def client_groups_parser(path):
    id_name_map = pd.read_csv(path)
    included_columns = ["Id", "Name"]
    id_name_map = id_name_map.loc[:, included_columns]
    id_name_map.set_index("Id", inplace=True)
    return id_name_map