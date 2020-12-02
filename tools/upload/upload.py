import logging
import os
import csv
from time import time

from tools.hbase.hbase_manager import HBaseManager


class Upload:

    def __init__(self, params):
        if len(params) < 2:
            logging.error("You are missing parameters")
            exit(1)
        # Inicializacion de parametros
        self.base_input = '/input'
        self.files_mult = int(params[0])
        self.columns_mult = int(params[1])
        self.input_file = params[2] if len(params) > 2 else None

        # Inicializacion del gestor de Hbase
        self.hbase_manager = HBaseManager()
        self.hbase_manager.table_name = "dataset"
        self.hbase_manager.get_table()

    def run_job(self):
        start_time1 = time()
        self.create_clean_table()
        if self.input_file:
            print(f"Processing the file:{self.input_file}")
            self.process_data(self.input_file)
        else:
            # Recorremos los ficheros de la carpeta input para procesarlos todos
            files_processed = 0
            for file in os.scandir(self.base_input):
                if file.is_file():
                    print(f"Processing the file:{file}")
                    self.process_data(file)
                    files_processed = files_processed + 1
            if files_processed > 0:
                print(f"Have been processed {files_processed} files")
            else:
                print("No files to process found in the /input folder")
        elapsed_time1 = time() - start_time1
        print(f"Data insertion lasted: {elapsed_time1}")

    """
    Este metodo elimina la tabla si ya existia y crea las familias de columnas necesarias, que son C + 1 familias.
    """
    def create_clean_table(self):
        self.hbase_manager.delete_table()
        families = []
        # Sumamos 2, ya que indice de las cf comienza en 1
        num_families = self.columns_mult + 2
        for fam_index in range(1, num_families):
            families.append(f'cf{fam_index}')
        self.hbase_manager.create_table(families)

    """
    Procesamos los datos de un fichero y los vamos insertando en la tabla batch.
    """
    def process_data(self, file):
        # Contador de lineas totales en el fichero, para crear el log de lineas procesadas
        with open(file, newline='') as csvfile:
            total_lines = sum(1 for line in csvfile) * self.files_mult

        with open(file, newline='') as csvfile:
            row_index = 1
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            batch_table = self.hbase_manager.get_batch_table()
            with batch_table as b_table:
                for row in spamreader:
                    for file_mult_index in range(1, self.files_mult + 1):
                        sensor = f'{file_mult_index}{row[0]}'
                        date = row[1]
                        measure = row[2]
                        estructura_columnas = {'cf1:sensor': sensor.encode('utf-8'),
                                               'cf1:dateTime': date.encode('utf-8')
                                            }
                        measure_index = 1
                        for column_measure_index in range(2, self.columns_mult + 2):
                            estructura_columnas[f'cf{column_measure_index}:measure{measure_index}'.encode('utf-8')] = measure.encode('utf-8')
                            measure_index = measure_index + 1
                        b_table.put(f'{file_mult_index}row{row_index}'.encode('utf-8'), estructura_columnas)
                        print(f"{row_index}/{total_lines} lines processed")
                        row_index = row_index + 1
            print(f"{row_index - 1} rows have been inserted successfully")
