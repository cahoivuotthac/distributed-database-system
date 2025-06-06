from contextlib import contextmanager
from cassandra.cluster import Cluster

def get_cassandra_cluster():
	try: 
		cluster = Cluster(
			['cassandra'],  # Use container name
			port=9042,
			protocol_version=4,
			connect_timeout=20,
			control_connection_timeout=20
		)
		return cluster 
	
	except Exception as e:
		print(f"Error when getting Cassandra hook: {str(e)}")
		return None
 
@contextmanager
def get_cassandra_session():
	"""Context manager for Cassandra session"""
	cluster = None
	session = None
	try:
		cluster = get_cassandra_cluster()
		if cluster:
			session = cluster.connect()
			session.execute("USE BTL2_data")
			session.default_timeout = 60  # Increase timeout for large operations
			yield session
		else:
			yield None
	except Exception as e:
		print(f"Error with Cassandra session: {e}")
		yield None
	finally:
		if session:
			session.shutdown()
		if cluster:
			cluster.shutdown()