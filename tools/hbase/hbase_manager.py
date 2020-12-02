import os

import happybase


class HBaseManager:
    def __init__(self):

        self.connection = happybase.Connection('localhost', port=9090, timeout=None, autoconnect=True)
        self.connection.open()
        self.table_name = None
        self.table = None
        self.table_batch = None


    def create_table(self, families):
        os.system(f"echo \"create '{self.table_name}', {families}\" | /build/hbase/bin/hbase shell -n")

    def create_split_table(self, families, row_keys):
        os.system(f"echo \"create '{self.table_name}', {families},  SPLITS=> {row_keys}\" | /build/hbase/bin/hbase shell -n")

    def put_row(self, row_key, columns):
        self.table.put(row_key, columns)

    def add_to_multiple_put(self, row_key, columns):
        self.table_batch.put(row_key, columns)

    def run_multiple_operations(self):
        self.table_batch.send()

    def get_batch_table(self):
        self.table_batch = self.table.batch(batch_size=1000)
        return self.table_batch

    def get_table(self):
        self.table = self.connection.table(self.table_name)

    def delete_table(self):
        tables = self.connection.tables()
        if self.table_name.encode('utf-8') in tables:
            self.connection.delete_table(self.table_name, True)
            print(f"The table {self.table_name} has been deleted")

    # Realiza un scan a la tabla inicializada con un filtro para el prefijo de la clave de la fila
    def get_data_scan_row_filter(self, row_index, column_index):
        data = self.table.scan(columns=['cf1', f'cf{column_index}'], row_prefix=f'{row_index}'.encode('utf-8'))
        return data

