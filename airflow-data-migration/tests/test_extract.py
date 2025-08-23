from etl_data_migration.extract_oracle_data import get_oracle_hook
import pytest 

@pytest.mark.integration 
def test_get_Oracle_hook():
    hook = get_oracle_hook()
    conn = hook.get_conn()
    
    cur = conn.cursor()
    cur.execute("SELECT 1 From dual")
    rs = cur.fetchone()
    assert rs[0] == 1
    
    cur.close()
    conn.close() 

@pytest.mark.integration
def test_query():
    hook = get_oracle_hook()
    conn = hook.get_conn()
    
    cur = conn.cursor()
    cur.execute(
		"""SELECT 
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
		ORDER BY cthd."MaHoaDon", hd."NgayTao" DESC"""
	)
    rs = cur.fetchall() # list of records 
    
    assert len(rs) > 0
    assert isinstance(rs[0][0], int) 
    
    cur.close()
    conn.close()
    





