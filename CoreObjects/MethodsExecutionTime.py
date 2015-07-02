__author__ = 'ruayhg'
from Parsers.SdtUtils import get_sdt_side, client_names_parser, client_groups_parser, root_path
from Parsers.SdtUtils import get_sdt_deal_flags_representation
import pandas as pd
import numpy as np

class MethodsExecutionTime(object):
    def __init__(self, order_exact_time):
        methods_execution_time = pd.DataFrame(columns=['Entry type', 'Time'])
        methods_execution_time = methods_execution_time.append(
            {'Entry type': 'Initial order', 'Time': order_exact_time},
            ignore_index = True)
        methods_execution_time.set_index('En')
        self.methods_execution_time = methods_execution_time

    def add_sdt_method_execution_time(self, entry_type, time):
        row = pd.Series([entry_type, time], index=['Entry type', 'Time'])
        self.methods_execution_time = self.methods_execution_time.append(row, ignore_index=True)