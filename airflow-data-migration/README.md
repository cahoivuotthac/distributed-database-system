**Add Oracle and Cassandra connections to Airflow**
```bash
docker exec airflow-webserver airflow connections add oracle \
  --conn-type oracle \
  --conn-host oracle-db \
  --conn-port 1521 \
  --conn-login DOAN_PHANTAN \
  --conn-password DONE \
  --conn-extra '{"service_name": "BTL1"}'
```

```bash
docker exec airflow-webserver airflow connections add cassandra \
	--conn-type cassandra \
	--conn-host cassandra-node1 \
	--conn-port 9042 \
	--conn-login cassandra \
	--conn-password cassandra \
	--conn-extra '{"keyspace": "BTL2_data"}'
```
<img src="https://github.com/user-attachments/assets/b93ec933-a54a-4ea5-827e-2e1e82011bb6" alt="image" width="800" height="480" />

