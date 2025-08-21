#!/bin/sh 

set -e 

echo "Waiting for Cassandra to be ready..."

# loop until connect successfully with cassandra-node1 
until cqlsh cassandra-node1 -e "DESCRIBE CLUSTER;" >/dev/null 2>&1; do
	sleep 5 
done 

echo "Cassandra is available"

if cqlsh cassandra-node1 -e "DESCRIBE KEYSPACE BTL2_data;" >/dev/null 2>&1; do 
	echo "Keyspace already exists. Skip init."
else 
	cqlsh cassandra-node1 -f /init.cql 
	echo "Schema created"
fi 