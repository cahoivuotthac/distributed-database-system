**Add Oracle and Cassandra connections to Airflow**
```
docker exec airflow-webserver airflow connections add oracle \
  --conn-type oracle \
  --conn-host oracle-db \
  --conn-port 1521 \
  --conn-login DOAN_PHANTAN \
  --conn-password DONE \
  --conn-extra '{"service_name": "BTL1"}'
```

```
docker exec airflow-webserver airflow connections add cassandra \
	--conn-type cassandra \
	--conn-host cassandra-node1 \
	--conn-port 9042 \
	--conn-login cassandra \
	--conn-password cassandra \
	--conn-extra '{"keyspace": "BTL2_data"}'
```