# hbase-data-tools
hbase-data-tools is a BigData academic project for the Computer Science Masters Degree which is offered at University Pablo de Olavide.

##Introduction
This is a simple project based in Hbase database, developed in python.
The project is a simple tool deployed in Docker that allow the loading of data into hbase table from a file, which is processed to increase the rows and columns to have a greater volume of data thus simulating a real example of big data and the extraction of the data by processing it through the MapReduce model to obtain an output file.


##Installation
To use the project through docker, you have to execute the following commands
 
docker build -t hbase-hacid .

docker run --name=hbase-docker -P -it -h hbase-docker -d -v "$input_path/input":/input -v "$output_path/output":/output hbase-hacid

####Tool commands
F -> veces que se va a repetir una linea en la inserccion de datos
C -> Veces que se va a repetir la columna en la tabla

- hacid upload F C
Creamos una tabla con los datos insertados. Por cada linea del fichero se repite F veces, y por cada medida del sensor, se añaden C columnas

- hacid extraxt F C
Extraemos todos los datos de repeticion de fila F y la columnas C, procesandolo con un MapReduce