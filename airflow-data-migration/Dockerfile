FROM apache/airflow:3.0.4

# only use user root for system-level packages like apt-get install 
USER root 
RUN apt-get update && apt-get install -y libaio1 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt 
COPY ./instantclient-basic-linux.x64-23.9.0.25.07 ./instantclient-basic-linux.x64-23.9.0.25.07
COPY ./instantclient-sqlplus-linux.x64-23.9.0.25.07 ./instantclient-sqlplus-linux.x64-23.9.0.25.07

# opt dir is owned by root, so only root user has permission 
# to create dirs and copy files there 
USER root
RUN mkdir -p /opt/instantclient_23_9 \
    && cp -r /opt/instantclient-basic-linux.x64-23.9.0.25.07/instantclient_23_9/* /opt/instantclient_23_9/ \
    && cp -r /opt/instantclient-sqlplus-linux.x64-23.9.0.25.07/instantclient_23_9/* /opt/instantclient_23_9/

USER airflow 
# set environment variables for Oracle Instant Client
ENV LD_LIBRARY_PATH=/opt/instantclient_23_9
ENV PATH=$PATH:/opt/instantclient_23_9 

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt