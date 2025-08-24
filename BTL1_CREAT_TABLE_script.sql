
CREATE TABLE "ChiNhanh" (
	"MaChiNhanh"  NUMBER(*, 0),
	"TenChiNhanh" NVARCHAR2(50),
	"DiaChi"	  NVARCHAR2(50),
	CONSTRAINT "PK_ChiNhanh" PRIMARY KEY ("MaChiNhanh") USING INDEX TABLESPACE USERS STORAGE (INITIAL 64 K
																							  MAXEXTENTS UNLIMITED)
)

CREATE TABLE "ChiTietHoaDon" (
	"MaHoaDon"  NUMBER(*, 0),
	"MaSanPham" NVARCHAR2(50),
	"SoLuong"   NUMBER(*, 0),
	"ThanhTien" NUMBER(*, 0),
	CONSTRAINT "PK_ChiTietHoaDon" PRIMARY KEY ("MaHoaDon", "MaSanPham") USING INDEX TABLESPACE USERS STORAGE (INITIAL 64 K
																											  MAXEXTENTS UNLIMITED)
)

CREATE TABLE "DanhMuc_SanPham" (
	"MaSanPham"	 NVARCHAR2(50),
	"TenDanhMuc" NVARCHAR2(50),
	CONSTRAINT PK_DANHMUC_SANPHAM PRIMARY KEY ("MaSanPham", "TenDanhMuc") USING INDEX TABLESPACE USERS STORAGE (INITIAL 64 K
																												MAXEXTENTS UNLIMITED)
)

CREATE TABLE "HoaDon" (
	"MaHoaDon"			  NUMBER(*, 0),
	"MaKhachHang"		  NUMBER(*, 0),
	"MaNhanVien"		  NUMBER(*, 0),
	"TongTien"			  NUMBER(*, 0),
	"NgayTao"			  DATE,
	"PhuongThucThanhToan" NVARCHAR2(50)
)

CREATE TABLE "KhachHang" (
	"MaKhachHang" NUMBER(*, 0),
	"Email"		  NVARCHAR2(50),
	"HoTen"		  NVARCHAR2(50),
	SDT			  NUMBER(*, 0),
	"DiaChi"	  NVARCHAR2(100),
	"GioiTinh"	  NVARCHAR2(50),
	"NgaySinh"	  DATE,
	CONSTRAINT "PK_KhachHang" PRIMARY KEY ("MaKhachHang") USING INDEX TABLESPACE USERS STORAGE (INITIAL 64 K
																								MAXEXTENTS UNLIMITED)
)

CREATE TABLE "NhanVien" (
	"MaNhanVien" NUMBER(*, 0),
	"MaChiNhanh" NUMBER(*, 0),
	"HoTen"		 NVARCHAR2(50),
	"GioiTinh"	 NVARCHAR2(50),
	"NgaySinh"	 DATE,
	SDT			 NUMBER(*, 0),
	"DiaChi"	 NVARCHAR2(100),
	"NgayVaoLam" DATE,
	"ChucVu"	 NVARCHAR2(50),
	"Luong"		 NUMBER(*, 0),
	CONSTRAINT "PK_NhanVien" PRIMARY KEY ("MaNhanVien") USING INDEX TABLESPACE USERS STORAGE (INITIAL 64 K
																							  MAXEXTENTS UNLIMITED)
)

CREATE TABLE "SanPham" (
	"MaSanPham"	 NVARCHAR2(50),
	"TenSanPham" NVARCHAR2(100),
	"Gia"		 NUMBER(*, 0),
	"TheLoai"	 NUMBER(*, 0),
	CONSTRAINT "PK_SanPham" PRIMARY KEY ("MaSanPham") USING INDEX TABLESPACE USERS STORAGE (INITIAL 64 K
																							MAXEXTENTS UNLIMITED)
)

CREATE TABLE "ThuocTinh_SanPham" (
	"MaSanPham"		  NVARCHAR2(50),
	"TenThuocTinh"	  NVARCHAR2(50),
	"GiaTriThuocTinh" NVARCHAR2(150)
)

CREATE TABLE "KhoSanPham_QLBanHang" (
	"MaSanPham"			 NVARCHAR2(50),
	"MaChiNhanh"		 NUMBER(*, 0),
	"TinhTrang"			 NVARCHAR2(50),
	"NgayCapNhat"		 DATE,
	"TongSoLuongDaBan"	 NUMBER(*, 0),
	"TongSoLuongDanhGia" NUMBER(*, 0),
	"TongSoLuongSao"	 NUMBER(*, 0)
)

CREATE TABLE "KhoSanPham_QLKho" (
	"MaSanPham"	  NVARCHAR2(50),
	"MaChiNhanh"  NUMBER(*, 0),
	"SoLuong"	  NUMBER(*, 0),
	"NgayCapNhat" DATE,
	CONSTRAINT "PK_KhoSanPham_QLKho" PRIMARY KEY ("MaSanPham", "MaChiNhanh") USING INDEX TABLESPACE USERS STORAGE (INITIAL 64 K
																												   MAXEXTENTS UNLIMITED)
)

TABLESPACE USERS
STORAGE (INITIAL 64 K
		 MAXEXTENTS UNLIMITED)
LOGGING;
