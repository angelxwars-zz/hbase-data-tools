import logging
import os
import sys

from tools.hbase.hbase_manager import HBaseManager


class CleanDB:

    def __init__(self, params):
        self.hbase_manager = HBaseManager()
        self.hbase_manager.table_name = "dataset"
        self.hbase_manager.get_table()

    def run_job(self):
        # Elimina la tabla dataset
        self.hbase_manager.delete_table()
