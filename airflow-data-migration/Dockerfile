FROM apache/airflow:3.0.4

USER root
RUN apt-get update && apt-get install -y libaio1 && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-oracle \
	cassandra-driver \
    oracledb 

RUN pip install --no-cache-dir --no-deps \
	apache-airflow-providers-apache-cassandra

WORKDIR /opt 
COPY ./instantclient-basic-linux.x64-23.9.0.25.07 ./instantclient-basic-linux.x64-23.9.0.25.07
COPY ./instantclient-sqlplus-linux.x64-23.9.0.25.07 ./instantclient-sqlplus-linux.x64-23.9.0.25.07

# Move all files from both folders into /opt/instantclient_23_9
USER root
RUN mkdir -p /opt/instantclient_23_9 \
    && cp -r /opt/instantclient-basic-linux.x64-23.9.0.25.07/instantclient_23_9/* /opt/instantclient_23_9/ \
    && cp -r /opt/instantclient-sqlplus-linux.x64-23.9.0.25.07/instantclient_23_9/* /opt/instantclient_23_9/

USER airflow
# Set environment variables for Oracle Instant Client
ENV LD_LIBRARY_PATH=/opt/instantclient_23_9
ENV PATH=$PATH:/opt/instantclient_23_9 


