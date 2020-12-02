# HBase with upload-extraction tool
# more documentation:
#
# Version 1.0

# http://docs.docker.io/en/latest/use/builder/

FROM ubuntu:18.04
MAINTAINER Angel Lozano <angelxwars@gmail.com>

ENV HBASE_VERSION=2.1.2
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/
#ENV HBASE_HOME=/build/hbase-$HBASE_VERSION
ENV HBASE_HOME=/build/hbase

RUN apt update \
    && apt -y install software-properties-common ssh openssh-server vim

# Install Java
RUN apt -y install openjdk-8-jdk  \
    && update-java-alternatives -s java-1.8.0-openjdk-amd64

# Install Python, Pip and HappyBase
RUN apt-get update && apt-get install -y python python3.6
RUN apt install -y python3-pip

# Download hbase
ADD https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz /build/
RUN tar -C /build/ -xzvf /build/hbase-$HBASE_VERSION-bin.tar.gz
RUN mkdir $HBASE_HOME
RUN mv /build/hbase-$HBASE_VERSION/* $HBASE_HOME
RUN rm -rf /build/hbase-$HBASE_VERSION
RUN rm -rf /build/hbase-${HBASE_VERSION}-bin.tar.gz

#Hbase configuration
COPY config/* /config/
COPY config/hbase-site.xml ${HBASE_HOME}/conf
COPY config/hbase-env.sh ${HBASE_HOME}/conf

#Hacid project
ADD tools/extract /hacid/tools/extract
ADD tools/hbase /hacid/tools/hbase
ADD tools/upload /hacid/tools/upload
ADD hacid.py /hacid/hacid.py
ADD requirements.txt /hacid/requirements.txt
RUN pip3 install -r /hacid/requirements.txt

#Copy scripts
ADD script/start-pseudo.sh /script/start-pseudo.sh

VOLUME /input
VOLUME /output

RUN echo "alias hbase=${HBASE_HOME}/bin/hbase" >> ~/.bashrc
RUN echo 'alias hacid="python3.6 /hacid/hacid.py"' >> ~/.bashrc

# Hbase ports
# Zookeeper Client Port
EXPOSE 2181
# HMaster Ports:
EXPOSE 16000 16001
# WebUI Port
EXPOSE 16010 16012 16013
# Region servers Ports
EXPOSE 16030 16032 16033 16034 16035

#RUN /script/start-pseudo.sh
CMD /bin/bash /script/start-pseudo.sh && /bin/bash
#CMD /bin/bash
