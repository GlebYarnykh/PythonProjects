__author__ = 'ruayhg'
import pandas as pd


class MethodsExecutionTime(object):
    def __init__(self, order_exact_time):
        methods_execution_time = pd.DataFrame(columns=['Entry type', 'Time'])
        methods_execution_time = methods_execution_time.append(
            {'Entry type': 'Initial order', 'Time': order_exact_time},
            ignore_index = True)
        self.methods_execution_time = methods_execution_time

    def add_sdt_method_execution_time(self, entry_type, time):
        row = pd.Series([entry_type, time], index=['Entry type', 'Time'])
        self.methods_execution_time = self.methods_execution_time.append(row, ignore_index=True)