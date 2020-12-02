import csv
import logging
import os
import shutil
import sys
from time import time

from tools.extract.sensor_to_line import SensorToLine
from tools.hbase.hbase_manager import HBaseManager


class Extract:

    HEADER = b'Sensor,Date,00:00,00:10,00:20,00:30,00:40,00:50,01:00,01:10,01:20,01:30,01:40,01:50,02:00,02:10,02:20,02:30,02:40,02:50,03:00,03:10,03:20,03:30,03:40,03:50,04:00,04:10,04:20,04:30,04:40,04:50,05:00,05:10,05:20,05:30,05:40,05:50,06:00,06:10,06:20,06:30,06:40,06:50,07:00,07:10,07:20,07:30,07:40,07:50,08:00,08:10,08:20,08:30,08:40,08:50,09:00,09:10,09:20,09:30,09:40,09:50,10:00,10:10,10:20,10:30,10:40,10:50,11:00,11:10,11:20,11:30,11:40,11:50,12:00,12:10,12:20,12:30,12:40,12:50,13:00,13:10,13:20,13:30,13:40,13:50,14:00,14:10,14:20,14:30,14:40,14:50,15:00,15:10,15:20,15:30,15:40,15:50,16:00,16:10,16:20,16:30,16:40,16:50,17:00,17:10,17:20,17:30,17:40,17:50,18:00,18:10,18:20,18:30,18:40,18:50,19:00,19:10,19:20,19:30,19:40,19:50,20:00,20:10,20:20,20:30,20:40,20:50,21:00,21:10,21:20,21:30,21:40,21:50,22:00,22:10,22:20,22:30,22:40,22:50,23:00,23:10,23:20,23:30,23:40,23:50\n'

    def __init__(self, params):
        if len(params) < 2:
            logging.error("You are missing parameters")
            exit(1)

        # Inicializacion de parametros
        self.base_input = '/input'
        self.map_reduce_output = '/output/mapreduce'
        self.final_file_output = '/output/output_dataset.csv'
        self.files_mult = int(params[0])
        self.columns_mult = int(params[1])
        self.temp_file = f'{self.base_input}/temp_file.txt'

        # Inicializacion del gestor de Hbase
        self.hbase_manager = HBaseManager()
        self.hbase_manager.table_name = "dataset"
        self.hbase_manager.get_table()

    def run_job(self):
        # Borramos el fichero antiguo de salida si existiera
        try:
            shutil.rmtree(self.final_file_output)
        except Exception:
            pass
        start_time1 = time()
        print(f"We are going to extract the rows with index F = {self.files_mult} and the sensor measurement C = {self.columns_mult}")
        # Sumamos a las columnas 1 por que empezamos las familias de columnas por 1 (ejemplo -> cf2:measure1)
        data = self.hbase_manager.get_data_scan_row_filter(self.files_mult, self.columns_mult + 1)
        with open(self.temp_file, 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',',
                       quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for key, data in data:
                sensor = data[b"cf1:sensor"].decode('utf-8')
                date = data[b"cf1:dateTime"].decode('utf-8').split(' ')[0]
                hour = data[b"cf1:dateTime"].decode('utf-8').split(' ')[1]
                mesure = data[f"cf{self.files_mult + 1}:measure{self.files_mult}".encode('utf-8')].decode('utf-8')
                spamwriter.writerow([sensor, date, hour, mesure])
        os.makedirs(self.map_reduce_output)
        print("Start MapReduce")
        # Ejecucion del MapReduce en local, apoyado en los runners que ofrece MrJob
        mr_job = SensorToLine(args=[self.temp_file, '--output-dir', self.map_reduce_output])
        with mr_job.make_runner() as runner:
            runner.run()
        elapsed_time1 = time() - start_time1
        print(f"Data extraction has lasted: {elapsed_time1}")

        # Unificamos todos los ficheros en uno solo
        with open(f'{self.final_file_output}', 'wb') as outfile:
            outfile.write(self.HEADER)
            for file in os.scandir(self.map_reduce_output):
                if file.name == 'output_dataset.csv':
                    continue
                with open(file, mode='r', newline='\n') as readfile:
                    infile = readfile.read()
                    lines = infile.split("\n")
                    lines.remove('')
                    for line in lines:
                        outfile.write(f'{line}\n'.encode('utf-8'))
        shutil.rmtree(self.map_reduce_output)
        os.remove(self.temp_file)

