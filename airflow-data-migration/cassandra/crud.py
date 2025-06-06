import os
import sys
from connect_2_clusters import connect_to_cluster
from cassandra.query import SimpleStatement
from tabulate import tabulate
from cassandra.cluster import Cluster

sys.path.append('/opt/airflow/scripts')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from load_to_cassandra import get_cassandra_session

cluster_ip = '26.103.246.194'
keyspace_name = 'btl2_data'

def crud_ops(query, cluster_sessions, table_name, condition, insert):
	stm = SimpleStatement(query)
	check_query = f"""
		SELECT COUNT(*) FROM {table_name} WHERE {condition};
		"""
  
	if insert:	
		cluster_sessions[0].execute(stm)
		print("Inserted successfully!")
		return True 
	
	success = False 
	for i in range(len(cluster_sessions)):
		try:
			tmp_check = cluster_sessions[i].execute(check_query)
			if tmp_check.one()[0] > 0:
				cluster_sessions[i].execute(stm)
				success = True 
				print(f'Operation is executed successfully in session {i}!')
			else:
				print(f'No matching records found in session {i}')
	
		except Exception as e:
			print(f"Error executing on remote session: {e}")
			continue 
	
	if not success:
		print("No matching data found in any session")
		return False
	
	return True 

def select_ops(query, cluster_sessions, table_name, condition):
	stm = SimpleStatement(query)
	success = False

	# Check if the main query uses ALLOW FILTERING
	uses_allow_filtering = 'ALLOW FILTERING' in query.upper()
	if condition:
		check_query = f"""
			SELECT COUNT(*) FROM {table_name} WHERE {condition}
		"""
		# Add ALLOW FILTERING to check query if main query uses it
		if uses_allow_filtering:
			check_query += " ALLOW FILTERING"
		check_query += ";"
  
	headers = None 
	all_rows = []
	for i in range(len(cluster_sessions)):
		try:
			tmp_check = cluster_sessions[i].execute(check_query)
			if tmp_check.one()[0] > 0:
				rows = cluster_sessions[i].execute(stm)
				if headers is None:
					headers = list(rows.column_names)
					
				for row in rows: 
					row_data = list(row)
					all_rows.append(row_data)
	 
				success = True 
				print(f'Operation is executed successfully in session {i}!')
			else:
				print(f'No matching records found in session {i}')
	
		except Exception as e:
			print(f"Error executing on remote session: {e}")
			continue 

	if not success:
		print("No matching data found in any session")
		return False
	
	if all_rows and headers:
		print(tabulate(all_rows, headers=headers, tablefmt='fancy_grid'))
	
	return True 

def select_union_ops(queries, cluster_sessions):
	stm = []
	if isinstance(queries, list):
		for query in queries: 
			prepared_stm = SimpleStatement(query)
			stm.append(prepared_stm)
  
	success = False
	
	headers = None 
	all_rows = []
	for i in range(len(cluster_sessions)):
		try:
			rows = cluster_sessions[i].execute(stm[i])
			if headers is None:
				headers = list(rows.column_names)
				
			for row in rows: 
				row_data = list(row)
				all_rows.append(row_data)
 
			success = True 
			print(f'Operation is executed successfully in session {i}!')
			
	
		except Exception as e:
			print(f"Error executing on remote session: {e}")
			continue 

	if not success:
		print("No matching data found in any session")
		return False
	
	if all_rows and headers:
		print(tabulate(all_rows, headers=headers, tablefmt='fancy_grid'))
	
	return True 

try: 
	remote_cluster, remote_session = connect_to_cluster(cluster_ip, keyspace_name)
	my_cluster, my_session = connect_to_cluster('127.0.0.1', keyspace_name)
	
	cluster_sessions = [
		my_session,
		remote_session
	]
 
	print("\n==========Kiểm tra kết nối 2 máy thuộc 2 cluster==========")
	
	# insert 
	# insert_query = f"""
	# INSERT INTO doanh_thu_moi_ngay_theo_ma_cn (
	# 	ma_chi_nhanh, 
	# 	ngay,
	# 	tong_tien
	# ) VALUES (
	# 	1,
	# 	'2021-01-02',
	# 	123000456
	# )
 	# """	
	# insert_rs = crud_ops(
	# 	query=insert_query,
	# 	cluster_sessions=[remote_session],
	# 	table_name='doanh_thu_moi_ngay_theo_ma_cn',
	# 	condition=None,
	# 	insert=True 
	# )
	# if not insert_rs:
	# 	print("Executed failed!") 
  
	# update
	# update_query = f"""
	# UPDATE {keyspace_name}.chi_tiet_hoa_don_theo_ma_kh
	# SET so_luong = 15
	# WHERE ma_khach_hang = 4317 AND ngay_tao = '2023-07-17 13:01:23.000000+0000'
	# 	AND ma_hoa_don = 58495
	# ;"""	
	# update_rs = crud_ops(
	# 	update_query,
	# 	cluster_sessions,
	# 	'chi_tiet_hoa_don_theo_ma_kh',
	# 	"ma_khach_hang = 4317 AND ngay_tao = '2023-07-17 13:01:23.000000+0000' AND ma_hoa_don = 58495",
	# 	insert=False
	# )
	# if not update_rs:
	# 	print("Executed failed!")
 
	# delete 
	# del_query = f"""
	# DELETE 
	# FROM {keyspace_name}.chi_tiet_hoa_don_theo_ma_kh
	# WHERE ma_khach_hang = 4317 AND ngay_tao = '2023-07-17 13:01:23.000000+0000' AND ma_hoa_don = 58495
	# """
	# del_rs = crud_ops(
	# 	del_query,
	# 	cluster_sessions,
	# 	'chi_tiet_hoa_don_theo_ma_kh',
	# 	"ma_khach_hang = 4317 AND ngay_tao = '2023-07-17 13:01:23.000000+0000' AND ma_hoa_don = 58495",
	# 	insert=False 
	# )
	# if not del_rs:
	# 	print("Executed failed!") 
	
	# select: partition key 
	# sel_part_query = f"""
	# SELECT * FROM chi_tiet_hoa_don_theo_ma_kh
	# WHERE ma_khach_hang = 1584;
	# """
	# sel_part_rs = select_ops(
	# 	sel_part_query,
	# 	cluster_sessions,
	# 	'chi_tiet_hoa_don_theo_ma_kh',
	# 	"ma_khach_hang = 1584"
	# )	
	# if not sel_part_rs:
	# 	print("Executed failed!")  
	
	# sel_part_clus_query = f"""
	# SELECT * FROM chi_tiet_hoa_don_theo_ma_cn
	# WHERE ma_chi_nhanh IN (1, 2) AND ngay = '2024-05-01';	
	# """
	# sel_part_clus_rs = select_ops(
	# 	sel_part_clus_query,
	# 	cluster_sessions,
	# 	'sl_khach_hang_moi_ngay_theo_ma_cn',
	# 	"ma_chi_nhanh IN (1, 2) AND ngay = '2024-05-01'"
	# )	
	# if not sel_part_clus_rs:
	# 	print("Executed failed!")
	
	# sel_part_clus_query_1 = f"""
	# SELECT * FROM kho_sp_theo_ma_cn
	# WHERE ma_chi_nhanh = 2 AND ma_san_pham = 'CCNPLT0021' AND tong_so_luong_ton_kho >= 23 AND tong_so_luong_ton_kho <= 64;	
	# """
	# sel_part_clus_query_2 = f"""
	# SELECT * FROM kho_sp_theo_ma_cn
	# WHERE ma_chi_nhanh = 1 AND ma_san_pham = 'CCNPLT0021' AND tong_so_luong_ton_kho >= 23 AND tong_so_luong_ton_kho <= 64;	
	# """
	# sel_part_clus_union_rs = select_union_ops(
	# 	[sel_part_clus_query_1, sel_part_clus_query_2],
	# 	cluster_sessions
	# )	
	# if not sel_part_clus_union_rs:
	# 	print("Executed failed!")
	
	sel_allow_filter_query = f"""
	SELECT * FROM chi_tiet_hoa_don_theo_ma_kh
	WHERE phuong_thuc_thanh_toan= 'Tiền Mặt'
	LIMIT 5
	ALLOW FILTERING;
	"""
	# sel_allow_filter_rs = select_ops(
	# 	sel_allow_filter_query,
	# 	cluster_sessions,
	# 	'chi_tiet_hoa_don_theo_ma_kh',
	# 	"phuong_thuc_thanh_toan= 'Tiền Mặt'"
	# )	
	# if not sel_allow_filter_rs:
	# 	print("Executed failed!")
  
	# Query with indexing 
	create_index_query = """
	CREATE INDEX IF NOT EXISTS ON doanh_thu_thang_nv_cn (tong_doanh_thu);
	"""
	try:
		for session in cluster_sessions:
			session.execute(create_index_query)
		print("Index created successfully!")
	except Exception as e:
		print(f"Index creation error (might already exist): {e}")
	
	index_query = """
	SELECT * FROM doanh_thu_thang_nv_cn
	WHERE tong_doanh_thu > 365000000
 	ALLOW FILTERING;
 	"""
	sel_index_rs = select_ops(
		index_query,
		cluster_sessions,
		'doanh_thu_thang_nv_cn',
		'tong_doanh_thu > 365000000'
	)
	if not sel_index_rs:
		print("Executed failed!")	
  
except Exception as e: 
	print(f"Error when doing queries: {e}")
 
finally:
	if 'remote_cluster' in locals():
		remote_cluster.shutdown()
	if 'my_cluster' in locals():
		my_cluster.shutdown()