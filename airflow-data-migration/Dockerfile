FROM apache/airflow:3.0.4

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-oracle \
	cassandra-driver \
    oracledb 

RUN pip install --no-cache-dir --no-deps \
	apache-airflow-providers-apache-cassandra