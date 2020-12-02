docker stop hbase-docker
docker rm hbase-docker
docker image rm hbase-hacid:1.0

docker build -t hbase-hacid:1.0 .

docker run --name=hbase-docker -P -it -h hbase-docker -d -v "G:\josea\Desktop\data-angel\input":/input -v "G:\josea\Desktop\data-angel\output":/output hbase-hacid:1.0
