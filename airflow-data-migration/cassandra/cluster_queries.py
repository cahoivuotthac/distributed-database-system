from connect_2_clusters import connect_to_cluster
from cassandra.query import SimpleStatement
from tabulate import tabulate

cluster_ip = '26.103.246.194'
keyspace_name = 'btl2_data'

try: 
	cluster, session = connect_to_cluster(cluster_ip, keyspace_name)

	print("\n==========Kiểm tra kết nối 2 máy thuộc 2 cluster==========")
 	
	query = "SELECT * FROM chi_tiet_hoa_don_theo_ma_kh WHERE ma_khach_hang IN (34, 38);"
	print(f"\nMáy 2 đang thực hiện câu truy vấn: {query}\n")

	statement = SimpleStatement(query)
	rows = session.execute(statement, trace=True)
	row_list = list(rows)  # <-- Chuyển sang list để đếm và xử lý

	print(f"Số dòng của kết quả trả về: {len(row_list)}")

	if not row_list:
		print("No results found")
	else:
		headers = rows.column_names
		print(tabulate(row_list, headers=headers, tablefmt='fancy_grid'))

		trace = rows.get_query_trace()
		print("\n--- Query tracing ---")
		for event in trace.events:
			print(f"{event.source} - {event.description} - {event.source_elapsed}μs")

except Exception as e: 
	print(f"Error when doing queries: {e}")
 
finally:
	if 'cluster' in locals():
		cluster.shutdown()
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  