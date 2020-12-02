import operator

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class SensorToLine(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol

    """
    Mapeamos todos los datos a procesar, teniendo como clave el sensor y la fecha y como valores la hora y la medida
    obtenida del sensor
    """
    def mapper(self, _, line):
        sensor = line.split(',')[0]
        date = line.split(',')[1]
        time = line.split(',')[2]
        measure = line.split(',')[3]
        yield [sensor, date], [time, measure]

    """
    Se procesan los datos, por cada key, ordenamos los valores segun el campo del valor time, y una vez ordenados, 
    los concatenamos en orden. El resultado es una key none, y el valor la concatenacion del sensor, dia y todas las medidas,
    para asi tener un fichero de texto plano y todos los campos concatenados en un string
    """
    def reducer(self, key, values):
        values_list = list(values)
        measure_sorted_list = sorted(values_list, key=operator.itemgetter(0))
        sensor_sin_numero = key[0][1:]
        fecha = key[1]
        result = f"{sensor_sin_numero},{fecha}"
        for measure in measure_sorted_list:
            result = f"{result},{measure[1]}"
        #     Es none para que no saque un diccionario, y en una linea nos ponga todos los valores que queremos
        yield None, result


if __name__ == '__main__':
    SensorToLine.run()