import os
import sys
from connect_2_clusters import connect_to_cluster
from crud import crud_ops, select_ops, select_union_ops

sys.path.append('/opt/airflow/scripts')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

cluster_ip = '26.103.246.194'
keyspace_name = 'btl2_data'

def display_menu():
	"""Hiển thị menu các thao tác CRUD"""
	print("\n" + "="*70)
	print("         MENU CÁC THAO TÁC CRUD TRÊN 2 CASSANDRA CLUSTER")
	print("="*70)
	print("1. Insert dữ liệu vào Máy 1 (Chi nhánh 1)")
	print("2. Update với dòng dữ liệu chỉ thuộc Máy 1 (Chi nhánh 1)")
	print("3. Xoá với dòng dữ liệu chỉ thuộc Máy 1 (Chi nhánh 1)")
	print("4. Truy vấn theo Partition Key")
	print("5. Truy vấn theo Partition + Clustering Key")
	print("6. Truy vấn theo partition key với range trên clustering key")
	print("7. Truy vấn với ALLOW FILTERING")
	print("8. Truy vấn với Secondary Index")
	print("0. Thoát")
	print("="*70)

def get_user_choice():
	"""Lấy và kiểm tra lựa chọn của người dùng"""
	while True:
		try:
			choice = int(input("Nhập lựa chọn của bạn (0-9): "))
			if 0 <= choice <= 9:
				return choice
			else:
				print("Lựa chọn không hợp lệ. Vui lòng nhập số từ 0-9.")
		except ValueError:
			print("Dữ liệu nhập không hợp lệ. Vui lòng nhập một số hợp lệ.")

def operation_1_insert(cluster_sessions):
	"""Thao tác 1: Thêm dữ liệu"""
	print("\n--- THAO TÁC THÊM DỮ LIỆU ---")
	insert_query = f"""
	INSERT INTO doanh_thu_moi_ngay_theo_ma_cn (
		ma_chi_nhanh, 
		ngay,
		tong_tien
	) VALUES (
		1,
		'2021-01-02',
		123000456
	)
	"""
	# print("Đang thực thi câu lệnh thêm dữ liệu...")
	insert_rs = crud_ops(
		query=insert_query,
		cluster_sessions=[cluster_sessions[1]],  # remote session
		table_name='doanh_thu_moi_ngay_theo_ma_cn',
		condition=None,
		insert=True 
	)
	if not insert_rs:
		print("Thao tác thêm dữ liệu thất bại!")
	else:
		print("Thao tác thêm dữ liệu thành công!")

def operation_2_update(cluster_sessions):
	"""Thao tác 2: Cập nhật dữ liệu"""
	print("\n--- THAO TÁC CẬP NHẬT DỮ LIỆU ---")
	update_query = f"""
	UPDATE {keyspace_name}.chi_tiet_hoa_don_theo_ma_kh
	SET so_luong = 15
	WHERE ma_khach_hang = 4317 AND ngay_tao = '2023-07-17 13:01:23.000000+0000'
		AND ma_hoa_don = 58495
	;"""
	# print("Đang thực thi câu lệnh cập nhật...")
	update_rs = crud_ops(
		update_query,
		cluster_sessions,
		'chi_tiet_hoa_don_theo_ma_kh',
		"ma_khach_hang = 4317 AND ngay_tao = '2023-07-17 13:01:23.000000+0000' AND ma_hoa_don = 58495",
		insert=False
	)
	if not update_rs:
		print("Thao tác cập nhật thất bại!")
	else:
		print("Thao tác cập nhật thành công!")

def operation_3_delete(cluster_sessions):
	"""Thao tác 3: Xóa dữ liệu"""
	print("\n--- THAO TÁC XÓA DỮ LIỆU ---")
	del_query = f"""
	DELETE 
	FROM {keyspace_name}.chi_tiet_hoa_don_theo_ma_kh
	WHERE ma_khach_hang = 4317 AND ngay_tao = '2023-07-17 13:01:23.000000+0000' AND ma_hoa_don = 58495
	"""
	# print("Đang thực thi câu lệnh xóa...")
	del_rs = crud_ops(
		del_query,
		cluster_sessions,
		'chi_tiet_hoa_don_theo_ma_kh',
		"ma_khach_hang = 4317 AND ngay_tao = '2023-07-17 13:01:23.000000+0000' AND ma_hoa_don = 58495",
		insert=False 
	)
	if not del_rs:
		print("Thao tác xóa thất bại!")
	else:
		print("Thao tác xóa thành công!")

def operation_4_select_partition(cluster_sessions):
	"""Thao tác 4: Truy vấn theo Partition Key"""
	print("\n--- TRUY VẤN THEO PARTITION KEY ---")
	sel_part_query = f"""
	SELECT * FROM chi_tiet_hoa_don_theo_ma_kh
	WHERE ma_khach_hang = 1584;
	"""
	# print("Đang thực thi truy vấn theo partition key...")
	sel_part_rs = select_ops(
		sel_part_query,
		cluster_sessions,
		'chi_tiet_hoa_don_theo_ma_kh',
		"ma_khach_hang = 1584"
	)
	if not sel_part_rs:
		print("Truy vấn theo partition key thất bại!")
	else:
		print("Truy vấn theo partition key thành công!")

def operation_5_select_partition_clustering(cluster_sessions):
	"""Thao tác 5: Truy vấn theo Partition + Clustering Key"""
	print("\n--- TRUY VẤN THEO PARTITION + CLUSTERING KEY ---")
	sel_part_clus_query = f"""
	SELECT * FROM sl_khach_hang_moi_ngay_theo_ma_cn
	WHERE ma_chi_nhanh IN (1, 2) AND ngay = '2024-05-01';
	"""
	# print("Đang thực thi truy vấn theo partition + clustering key...")
	sel_part_clus_rs = select_ops(
		sel_part_clus_query,
		cluster_sessions,
		'sl_khach_hang_moi_ngay_theo_ma_cn',
		"ma_chi_nhanh IN (1, 2) AND ngay = '2024-05-01'"
	)
	if not sel_part_clus_rs:
		print("Truy vấn theo partition + clustering key thất bại!")
	else:
		print("Truy vấn theo partition + clustering key thành công!")

def operation_6_select_range(cluster_sessions):
	"""Thao tác 6: Truy vấn với Range trên Clustering Key"""
	print("\n--- TRUY VẤN VỚI RANGE TRÊN CLUSTERING KEY ---")
	sel_part_clus_query_1 = f"""
	SELECT * FROM kho_sp_theo_ma_cn
	WHERE ma_chi_nhanh = 2 AND ma_san_pham = 'CCNPLT0021' AND tong_so_luong_ton_kho >= 23 AND tong_so_luong_ton_kho <= 64;
	"""
	sel_part_clus_query_2 = f"""
	SELECT * FROM kho_sp_theo_ma_cn
	WHERE ma_chi_nhanh = 1 AND ma_san_pham = 'CCNPLT0021' AND tong_so_luong_ton_kho >= 23 AND tong_so_luong_ton_kho <= 64;
	"""
	# print("Đang thực thi truy vấn range trên cả hai cluster...")
	sel_part_clus_union_rs = select_union_ops(
		[sel_part_clus_query_1, sel_part_clus_query_2],
		cluster_sessions
	)
	if not sel_part_clus_union_rs:
		print("Truy vấn range thất bại!")
	else:
		print("Truy vấn range hoàn thành thành công!")

def operation_7_allow_filtering(cluster_sessions):
	"""Thao tác 7: Truy vấn với ALLOW FILTERING"""
	print("\n--- TRUY VẤN VỚI ALLOW FILTERING ---")
	print("Cảnh báo: ALLOW FILTERING không được khuyến khích sử dụng trong môi trường production!")
	sel_allow_filter_query = f"""
	SELECT * FROM chi_tiet_hoa_don_theo_ma_kh
	WHERE phuong_thuc_thanh_toan= 'Tiền Mặt'
	LIMIT 5
	ALLOW FILTERING;
	"""
	# print("Đang thực thi truy vấn ALLOW FILTERING...")
	sel_allow_filter_rs = select_ops(
		sel_allow_filter_query,
		cluster_sessions,
		'chi_tiet_hoa_don_theo_ma_kh',
		"phuong_thuc_thanh_toan= 'Tiền Mặt'"
	)
	if not sel_allow_filter_rs:
		print("Truy vấn ALLOW FILTERING thất bại!")
	else:
		print("Truy vấn ALLOW FILTERING thành công!")

def operation_8_secondary_index(cluster_sessions):
	"""Thao tác 8: Truy vấn với Secondary Index"""
	print("\n--- TRUY VẤN VỚI SECONDARY INDEX ---")
	print("Cảnh báo: Secondary index có hạn chế trong Cassandra!")
	
	# Tạo index trước
	create_index_query = """
	CREATE INDEX IF NOT EXISTS ON doanh_thu_thang_nv_cn (tong_doanh_thu);
	"""
	try:
		print("Đang tạo secondary index...")
		for session in cluster_sessions:
			session.execute(create_index_query)
		print("Tạo index thành công!")
	except Exception as e:
		print(f"Lỗi tạo index (có thể đã tồn tại): {e}")
	
	# Thực thi truy vấn sử dụng index
	index_query = """
	SELECT * FROM doanh_thu_thang_nv_cn
	WHERE tong_doanh_thu > 365000000
	ALLOW FILTERING;
	"""
	# print("Đang thực thi truy vấn secondary index...")
	sel_index_rs = select_ops(
		index_query,
		cluster_sessions,
		'doanh_thu_thang_nv_cn',
		'tong_doanh_thu > 365000000'
	)
	if not sel_index_rs:
		print("Truy vấn secondary index thất bại!")
	else:
		print("Truy vấn secondary index thành công!")

def main_menu():
	try:
		print("Đang kết nối đến Cassandra cluster...")
		remote_cluster, remote_session = connect_to_cluster(cluster_ip, keyspace_name)
		my_cluster, my_session = connect_to_cluster('127.0.0.1', keyspace_name)
		
		cluster_sessions = [my_session, remote_session]
	  
		operations = {
			1: operation_1_insert,
			2: operation_2_update,
			3: operation_3_delete,
			4: operation_4_select_partition,
			5: operation_5_select_partition_clustering,
			6: operation_6_select_range,
			7: operation_7_allow_filtering,
			8: operation_8_secondary_index
		}
		
		while True:
			display_menu()
			choice = get_user_choice()
			
			if choice == 0:
				print("\nĐang thoát ... Cảm ơn!")
				break
			elif choice in operations:
				try:
					operations[choice](cluster_sessions)
				except Exception as e:
					print(f"Lỗi khi thực thi thao tác: {e}")
				
				input("\nNhấn Enter để quay lại menu chính...")
			else:
				print("Lựa chọn không hợp lệ. Vui lòng thử lại.")
	
	except Exception as e:
		print(f"Lỗi khi kết nối hoặc thực thi thao tác: {e}")
	
	finally:
		if 'remote_cluster' in locals():
			remote_cluster.shutdown()
			print("Đã đóng kết nối cluster của Công Phan.")
		if 'my_cluster' in locals():
			my_cluster.shutdown()
			print("Đã đóng kết nối cluster local.")

if __name__ == "__main__":
	main_menu()