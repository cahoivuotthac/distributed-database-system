from datetime import datetime
import pandas as pd
import json
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.configuration import conf

def get_oracle_hook():
	try: 
		return OracleHook(oracle_conn_id='oracle')
	except Exception as e:
		print(f"Error when getting oracle hook: {e}")

def execute_query(query, file_name):
	try:
		oracle_hook = get_oracle_hook()
		data = pd.read_sql( # return DataFrame 
			query, 
			oracle_hook.get_conn() 
		)
		
		print(f"Successfully extracted {len(data)} rows from Oracle")
		if len(data) > 0:
			print(f"Columns: {data.columns.tolist()}")

		data.to_parquet(
			file_name,
			index=False
		)

	except Exception as e: 
		print(f"Error extracting Oracle data: {e}")
		raise # stop the current workflow  

def extract_invoice_data():
	invoice_query = f"""
		SELECT 
			hd."MaKhachHang",
			cthd."MaHoaDon",
			cthd."MaSanPham",
			cthd."SoLuong",
			cthd."ThanhTien",
			hd."TongTien",
			hd."NgayTao",
			hd."PhuongThucThanhToan",
			hd."MaNhanVien"
		FROM "DOAN_PHANTAN"."ChiTietHoaDon" cthd 
			JOIN "DOAN_PHANTAN"."HoaDon" hd ON cthd."MaHoaDon" = hd."MaHoaDon"
			JOIN "DOAN_PHANTAN"."KhachHang" kh ON hd."MaKhachHang" = kh."MaKhachHang"
		ORDER BY cthd."MaHoaDon", hd."NgayTao" DESC
	"""
	
	print("Extracting invoice data...")
	df = execute_query(invoice_query, 'chi_tiet_hoa_don_theo_makh.parquet')

def extract_revenue_branch_data():	
	revenue_query = """
		SELECT 
			cn."MaChiNhanh",
			TRUNC(hd."NgayTao") AS "Ngay",
			SUM(hd."TongTien") AS "TongTien"
		FROM "DOAN_PHANTAN"."HoaDon" hd 
			JOIN "DOAN_PHANTAN"."NhanVien" nv ON hd."MaNhanVien" = nv."MaNhanVien"
			JOIN "DOAN_PHANTAN"."ChiNhanh" cn ON cn."MaChiNhanh" = nv."MaChiNhanh"
		GROUP BY cn."MaChiNhanh", TRUNC(hd."NgayTao")
		ORDER BY TRUNC(hd."NgayTao") DESC
	"""
	
	print("Extracting revenue data...")
	df = execute_query(revenue_query, 'doanh_thu_moi_ngay_theo_macn.parquet')

def extract_warehouse_data():
	
	warehouse_query = """
		SELECT 
			kspq."MaChiNhanh",
			kspq."MaSanPham",
			sp."TenSanPham",
			kspqh."TinhTrang",
			kspqh."TongSoLuongDanhGia",
			kspqh."TongSoLuongDaBan",
			kspq."SoLuong"
		FROM "DOAN_PHANTAN"."KhoSanPham_QLBanHang" kspqh, 
			 "DOAN_PHANTAN"."KhoSanPham_QLKho" kspq, 
			 "DOAN_PHANTAN"."SanPham" sp
		WHERE  kspqh."MaSanPham" = kspq."MaSanPham" 
		  AND sp."MaSanPham" =  kspq."MaSanPham"
		ORDER BY  kspq."MaSanPham",  kspq."SoLuong"
	"""
	
	print("Extracting warehouse data...")
	df = execute_query(warehouse_query, 'kho_sp_theo_ma_cn.parquet')

def extract_customer_data():
	cus_query = """
		SELECT 
		  nv."MaChiNhanh",
		  TRUNC(hd."NgayTao") AS Ngay,
		  COUNT(DISTINCT hd."MaKhachHang") AS SoLuongKhachHang
		FROM "DOAN_PHANTAN"."NhanVien" nv
		  JOIN "DOAN_PHANTAN"."HoaDon" hd ON nv."MaNhanVien" = hd."MaNhanVien"
		GROUP BY nv."MaChiNhanh", TRUNC(hd."NgayTao")
		ORDER BY Ngay DESC
	"""
	
	print("Extracting customer data...")
	df = execute_query(cus_query, 'sl_khach_hang_moi_ngay_theo_macn.parquet')
	
def extract_doanh_thu_sp_quy_cn():

	cus_query = """
		SELECT
		  KQL."MaChiNhanh",
		  SP."MaSanPham",
		  EXTRACT(YEAR FROM HD."NgayTao") AS Nam,
		  TO_NUMBER(TO_CHAR(HD."NgayTao", 'Q')) AS Quy,
		  SUM(CT."ThanhTien") AS DoanhThu
		FROM
		  "HoaDon" HD
		  JOIN "ChiTietHoaDon" CT ON HD."MaHoaDon" = CT."MaHoaDon"
		  JOIN "SanPham" SP ON SP."MaSanPham" = CT."MaSanPham"
		  JOIN "NhanVien" NV ON NV."MaNhanVien" = HD."MaNhanVien"
		  JOIN "ChiNhanh" KQL ON NV."MaChiNhanh" = KQL."MaChiNhanh"
		GROUP BY
		  KQL."MaChiNhanh",
		  SP."MaSanPham",
		  EXTRACT(YEAR FROM HD."NgayTao"),
		  TO_NUMBER(TO_CHAR(HD."NgayTao", 'Q'))
		ORDER BY
		  KQL."MaChiNhanh", SP."MaSanPham", Nam, Quy
	"""  
	
	print("Extracting doanh_thu_sp_quy_cn data...")
	df = execute_query(cus_query, 'doanh_thu_sp_trong_quy_nam_theo_macn.parquet')
	
def extract_doanh_thu_thang_nv_cn():

	cus_query = """
		SELECT
		  NV."MaChiNhanh",
		  NV."MaNhanVien",
		  EXTRACT(YEAR FROM HD."NgayTao") AS Nam,
		  EXTRACT(MONTH FROM HD."NgayTao") AS Thang,
		  SUM(CT."ThanhTien") AS DoanhThu
		FROM
		  "HoaDon" HD
		  JOIN "ChiTietHoaDon" CT ON HD."MaHoaDon" = CT."MaHoaDon"
		  JOIN "NhanVien" NV ON HD."MaNhanVien" = NV."MaNhanVien"
		GROUP BY
		  NV."MaChiNhanh",
		  NV."MaNhanVien",
		  EXTRACT(YEAR FROM HD."NgayTao"),
		  EXTRACT(MONTH FROM HD."NgayTao")
		ORDER BY
		  NV."MaChiNhanh", NV."MaNhanVien", Nam, Thang
	"""  

	print("Extracting doanh_thu_thang_nv_cn data...")
	df = execute_query(cus_query, 'doanh_thu_nv_tung_thang_theo_macn.parquet')