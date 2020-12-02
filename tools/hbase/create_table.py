from tools.hbase.hbase_manager import HBaseManager


class CreateTable:

    def __init__(self, params):
        self.families = params[0]
        # Format: ['key1', 'key2']
        self.split_keys = params[1] if len(params) == 2 else None

        self.hbase_manager = HBaseManager()
        self.hbase_manager.table_name = "dataset"
        self.hbase_manager.get_table()

    def run_job(self):
        # Tenemos dos formas de crear una tabla, con o sin pre-split
        if self.split_keys:
            self.hbase_manager.create_split_table(self.families, self.split_keys)
        else:
            self.hbase_manager.create_table(self.families)


