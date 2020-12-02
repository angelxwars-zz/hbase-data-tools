# hbase-data-tools
hbase-data-tools is a BigData academic project for the Computer Science Masters Degree which is offered at University Pablo de Olavide.

## Introduction
This is a simple project based in Hbase database, developed in python.
The project is a simple tool deployed in Docker that allow the loading of data into hbase table from a file, which is processed to increase the rows and columns to have a greater volume of data thus simulating a real example of big data and the extraction of the data by processing it through the MapReduce model to obtain an output file.


## Installation
To use the project through docker, you have to execute the following commands
 
docker build -t hbase-hacid .

docker run --name=hbase-docker -P -it -h hbase-docker -d -v "$input_path/input":/input -v "$output_path/output":/output hbase-hacid

#### Tool commands
F -> times a line will be repeated in the data insertion

C -> Times the column is going to be repeated in the table


- hacid upload F C

We create a table with the inserted data. For each line of the file, F times are repeated, and for each sensor measurement, C columns are added

- hacid extraxt F C

We extract all the repetition data from row F and column C, processing it with a MapReduce
