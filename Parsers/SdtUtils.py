import pandas as pd

__author__ = 'ruayhg'
root_path = 'C:\\Users\\ruayhg\\PycharmProjects\\PythonProjects\\'

def get_sdt_side(side_int):
    if side_int == 0:
        return "Buy"
    elif side_int ==1:
        return "Sell"
    else:
        return "Unknown side"

def get_sdt_deal_flags_representation(flag_int):
    if flag_int == 0:
        return None
    elif flag_int == 1:
        return "FOK"
    elif flag_int == 2:
        return "CounterCCY"
    elif flag_int == 4:
        return "Market"
    elif flag_int == 8:
        return "Don't validate"
    elif flag_int == 16:
        return "Deal from another system"
    else:
        return "Unknown Flag"

def client_names_parser(path):
    id_name_map = pd.read_csv(path, sep=';', encoding = 'ISO-8859-1')
    included_columns = ["Id", "Name", "Account Name"]
    id_name_map = id_name_map.loc[:, included_columns]
    id_name_map["Id"] = id_name_map["Id"].apply(int)
    id_name_map.set_index("Id", inplace=True)
    return id_name_map

def client_groups_parser(path):
    id_name_map = pd.read_csv(path, sep=';', encoding = 'ISO-8859-1')
    included_columns = ["Id", "Name"]
    id_name_map = id_name_map.loc[:, included_columns]
    id_name_map["Id"] = id_name_map["Id"].apply(int)
    id_name_map.set_index("Id", inplace=True)
    return id_name_map
