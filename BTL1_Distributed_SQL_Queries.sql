ALTER SESSION SET CURRENT_SCHEMA = BTL1;
ALTER SESSION SET "_ORACLE_SCRIPT"=TRUE;

/* CHƯƠNG 1*/

/*
Query 1:
Tại chi nhánh 1, với role là GiamDoc:
Liệt kê danh sách các nhân viên của tất cả chi nhánh có mức lương cao hơn 25 triệu (dùng UNION)
*/
SELECT "MaNhanVien", "HoTen"
FROM BTL1."NhanVien"
WHERE "Luong" > 25000000
UNION
SELECT "MaNhanVien", "HoTen"
FROM BTL1."NhanVien"@GiamDoc12Link
WHERE "Luong" > 25000000
UNION
SELECT "MaNhanVien", "HoTen"
FROM BTL1."NhanVien"@GiamDoc13Link
WHERE "Luong" > 25000000

/*
Query 2:
Tại chi nhánh 3, với role là QuanLyKho:
Liệt kê những sản phẩm trong ngày 30/4/2025 có số lượng bán ra lớn hơn so với
sản phẩm có số lượng bán ra trung bình ở chi nhánh 1 trong cùng ngày này.
*/
SELECT SP."MaSanPham", SP."TenSanPham", TK."TongSoLuongDaBan"
FROM (
  SELECT QLBH."MaSanPham", QLBH."TongSoLuongDaBan"
  FROM BTL1."KhoSanPham_QLBanHang" QLBH
  WHERE TRUNC(QLBH."NgayCapNhat") = TO_DATE('30/04/2025', 'DD/MM/YYYY') AND QLBH."TongSoLuongDaBan" > (
                    SELECT AVG(QLBH1."TongSoLuongDaBan")
                    FROM BTL1."KhoSanPham_QLBanHang"@QuanLyKho31Link QLBH1
                    WHERE TRUNC(QLBH1."NgayCapNhat") = TO_DATE('30/04/2025', 'DD/MM/YYYY')
                )
) TK JOIN BTL1."SanPham" SP ON TK."MaSanPham" = SP."MaSanPham";

/*
Query 3:
Tại chi nhánh 2, với role là NhanVien:
Liệt kê những khách hàng đã từng mua từ 7 sản phẩm trở lên thuộc danh mục "Cây Trong Nhà" ở cả 3 chi nhánh.
*/
/*
(SELECT KH3."MaKhachHang", KH3."HoTen"
 FROM BTL1."KhachHang" KH3
 WHERE NOT EXISTS (SELECT *
                   FROM BTL1."DanhMuc_SanPham" DMSP3
                   WHERE DMSP3."TenDanhMuc" = 'Cây Trong Nhà' 
                   AND NOT EXISTS(SELECT *
                                  FROM BTL1."HoaDon" HD3 JOIN BTL1."ChiTietHoaDon" CTHD3 ON HD3."MaHoaDon" = CTHD3."MaHoaDon"
                                  WHERE CTHD3."MaSanPham" = DMSP3."MaSanPham" AND HD3."MaKhachHang" = KH3."MaKhachHang"
                                 )
                  )
)
INTERSECT

(SELECT KH1."MaKhachHang", KH1."HoTen"
 FROM BTL1."KhachHang" KH1
 WHERE NOT EXISTS (SELECT *
                   FROM BTL1."DanhMuc_SanPham" DMSP1
                   WHERE DMSP1."TenDanhMuc" = 'Cây Trong Nhà' 
                   AND NOT EXISTS(SELECT *
                                  FROM BTL1."HoaDon"@NhanVien31Link HD1 
                                  JOIN BTL1."ChiTietHoaDon"@NhanVien31Link CTHD1 ON HD1."MaHoaDon" = CTHD1."MaHoaDon"
                                  WHERE CTHD1."MaSanPham" = DMSP1."MaSanPham" AND HD1."MaKhachHang" = KH1."MaKhachHang"
                                  )
                  )
)
INTERSECT

(SELECT KH2."MaKhachHang", KH2."HoTen"
 FROM BTL1."KhachHang" KH2
 WHERE NOT EXISTS (SELECT *
                   FROM BTL1."DanhMuc_SanPham" DMSP2
                   WHERE DMSP2."TenDanhMuc" = 'Cây Trong Nhà' 
                   AND NOT EXISTS(SELECT *
                                  FROM BTL1."HoaDon"@NhanVien32Link HD2 
                                  JOIN BTL1."ChiTietHoaDon"@NhanVien32Link CTHD2 ON HD2."MaHoaDon" = CTHD2."MaHoaDon"
                                  WHERE CTHD2."MaSanPham" = DMSP2."MaSanPham" AND HD2."MaKhachHang" = KH2."MaKhachHang"
                                  )
                  )
)
*/

WITH "TatCaSanPham_CTN" AS (
  SELECT DISTINCT "MaSanPham"
  FROM BTL1."DanhMuc_SanPham" dmsp
  WHERE dmsp."TenDanhMuc" = 'Cây Trong Nhà'
), 

"KhachHang_SanPhamTM_C1" AS (
  SELECT hd."MaKhachHang", cthd."MaSanPham"
  FROM BTL1."HoaDon" hd
  JOIN BTL1."ChiTietHoaDon" cthd ON hd."MaHoaDon" = cthd."MaHoaDon"
  WHERE cthd."MaSanPham" IN (
      SELECT "MaSanPham"
      FROM "TatCaSanPham_CTN"
    )
),

"KhachHang_SanPhamTM_C2" AS (
  SELECT hd."MaKhachHang", cthd."MaSanPham"
  FROM BTL1."HoaDon"@NhanVien21Link hd
  JOIN BTL1."ChiTietHoaDon"@NhanVien21Link cthd ON hd."MaHoaDon" = cthd."MaHoaDon"
  WHERE cthd."MaSanPham" IN (
      SELECT "MaSanPham"
      FROM "TatCaSanPham_CTN"
    )
),

"KhachHang_SanPhamTM_C3" AS (
  SELECT hd."MaKhachHang", cthd."MaSanPham"
  FROM BTL1."HoaDon"@NhanVien23Link hd
  JOIN BTL1."ChiTietHoaDon"@NhanVien23Link cthd ON hd."MaHoaDon" = cthd."MaHoaDon"
  WHERE cthd."MaSanPham" IN (
      SELECT "MaSanPham"
      FROM "TatCaSanPham_CTN"
    )
),

"KhachMuaDayDu_C1" AS (
  SELECT khsp."MaKhachHang"
  FROM "KhachHang_SanPhamTM_C1" khsp
  GROUP BY khsp."MaKhachHang"
  HAVING COUNT(DISTINCT khsp."MaSanPham") >= 7
),

"KhachMuaDayDu_C2" AS (
  SELECT khsp."MaKhachHang"
  FROM "KhachHang_SanPhamTM_C2" khsp
  GROUP BY khsp."MaKhachHang"
  HAVING COUNT(DISTINCT khsp."MaSanPham") >= 7
),

"KhachMuaDayDu_C3" AS (
  SELECT khsp."MaKhachHang"
  FROM "KhachHang_SanPhamTM_C3" khsp
  GROUP BY khsp."MaKhachHang"
  HAVING COUNT(DISTINCT khsp."MaSanPham") >= 7
)

SELECT kh."MaKhachHang", kh."HoTen"
FROM(
  SELECT "MaKhachHang"
  FROM "KhachMuaDayDu_C1"
  INTERSECT
  SELECT "MaKhachHang"
  FROM "KhachMuaDayDu_C2"
  INTERSECT
  SELECT "MaKhachHang"
  FROM "KhachMuaDayDu_C3"
) Table_temp JOIN BTL1."KhachHang" kh ON kh."MaKhachHang" = Table_temp."MaKhachHang";

-----------------------------------------------
/*
Query 4:
Tại chi nhánh 1, với role là NhanVien:
Liệt kê các hóa đơn có tổng tiền lớn hơn giá trị tổng tiền trung bình của tất cả hóa đơn theo từng chi nhánh trong ngày 08/03/2024 (dùng AVG).
*/
SELECT HD."MaHoaDon", HD."TongTien", TRUNC(HD."NgayTao") NgayHoaDon
FROM BTL1."HoaDon" HD
WHERE TRUNC(HD."NgayTao") = TO_DATE('08/03/2024', 'DD/MM/YYYY') 
AND HD."TongTien" > ( SELECT AVG(HD1."TongTien")
                      FROM BTL1."HoaDon" HD1
                      WHERE TRUNC(HD1."NgayTao") = TO_DATE('08/03/2024', 'DD/MM/YYYY')
                     )
AND HD."TongTien" > ( SELECT AVG(HD2."TongTien")
                      FROM BTL1."HoaDon"@NhanVien12Link HD2
                      WHERE TRUNC(HD2."NgayTao") = TO_DATE('08/03/2024', 'DD/MM/YYYY')
                     )
AND HD."TongTien" > ( SELECT AVG(HD3."TongTien")
                      FROM BTL1."HoaDon"@NhanVien13Link HD3
                      WHERE TRUNC(HD3."NgayTao") = TO_DATE('08/03/2024', 'DD/MM/YYYY')
                     )

/*
Query 5:
Tại chi nhánh 2, với role là NhanVien,
Liệt kê danh sách khách hàng được xem là VIP (có tổng số tiền mua hàng trên 20 triệu đồng) tính trên toàn bộ hệ thống 3 chi nhánh
*/
SELECT 
  t."MaKhachHang",
  t."HoTen",
  SUM(t."TongTien") AS TongTienMuaHang 
FROM (
  SELECT kh."MaKhachHang", kh."HoTen", hd."TongTien"
  FROM BTL1."KhachHang" kh 
  JOIN BTL1."HoaDon" hd ON kh."MaKhachHang" = hd."MaKhachHang"
  
  UNION ALL

  SELECT kh1."MaKhachHang", kh1."HoTen", hd1."TongTien"
  FROM BTL1."KhachHang" kh1 
  JOIN BTL1."HoaDon"@NhanVien21Link hd1 ON kh1."MaKhachHang" = hd1."MaKhachHang"

  UNION ALL

  SELECT kh2."MaKhachHang", kh2."HoTen", hd2."TongTien"
  FROM BTL1."KhachHang" kh2 
  JOIN BTL1."HoaDon"@NhanVien23Link hd2 ON kh2."MaKhachHang" = hd2."MaKhachHang"
  
) t
GROUP BY t."MaKhachHang", t."HoTen"
HAVING SUM(t."TongTien") > 20000000
ORDER BY TongTienMuaHang DESC;

/*
Query 6:
Tại chi nhánh 2, với role là QuanLyKho, thống kê tổng số lượng tồn kho của từng loại sản phẩm tại cả 3 chi nhánh
*/
SELECT 
  t."MaSanPham", 
  SUM(t."SoLuong") AS TongHangTonKho
FROM (
  SELECT "MaSanPham", "SoLuong"
  FROM BTL1."KhoSanPham_QLKho"
  
  UNION ALL

  SELECT "MaSanPham", "SoLuong"
  FROM BTL1."KhoSanPham_QLKho"@QuanLyKho21Link
  
  UNION ALL 

  SELECT "MaSanPham", "SoLuong"
  FROM BTL1."KhoSanPham_QLKho"@QuanLyKho23Link
 
) t 
GROUP BY t."MaSanPham"
ORDER BY TongHangTonKho DESC;

/*
Query 7:
Tại chi nhánh 2, với role là NhanVien
Cho biết danh sách các sản phẩm có tổng số lượng bán ra trong tháng hiện tại (tháng tại thời điểm thực hiện câu truy vấn)
giảm hơn 30% so với tháng trước trong năm 2025 tính trên toàn 3 chi nhánh
*/
WITH "DoanhSoToanBo" AS ( 
  SELECT 
    cthd."MaSanPham",
    EXTRACT(MONTH FROM hd."NgayTao") AS "Thang",
    SUM(cthd."SoLuong") AS "TongSoLuongBan"
  FROM BTL1."ChiTietHoaDon" cthd
  JOIN BTL1."HoaDon" hd ON cthd."MaHoaDon" = hd."MaHoaDon"
  WHERE EXTRACT(YEAR FROM hd."NgayTao") = 2025
  GROUP BY 
    cthd."MaSanPham", 
    EXTRACT(MONTH FROM hd."NgayTao")
  
  UNION ALL

  SELECT 
    cthd."MaSanPham",
    EXTRACT(MONTH FROM hd."NgayTao") AS "Thang",
    SUM(cthd."SoLuong")
  FROM BTL1."ChiTietHoaDon"@NhanVien21Link cthd
  JOIN BTL1."HoaDon"@NhanVien21Link hd ON cthd."MaHoaDon" = hd."MaHoaDon"
  WHERE EXTRACT(YEAR FROM hd."NgayTao") = 2025
  GROUP BY 
    cthd."MaSanPham", 
    EXTRACT(MONTH FROM hd."NgayTao")

  UNION ALL

  SELECT 
    cthd."MaSanPham",
    EXTRACT(MONTH FROM hd."NgayTao") AS "Thang",
    SUM(cthd."SoLuong")
  FROM BTL1."ChiTietHoaDon"@NhanVien23Link cthd
  JOIN BTL1."HoaDon"@NhanVien23Link hd ON cthd."MaHoaDon" = hd."MaHoaDon"
  WHERE EXTRACT(YEAR FROM hd."NgayTao") = 2025
  GROUP BY 
    cthd."MaSanPham", 
    EXTRACT(MONTH FROM hd."NgayTao")
),

"ThangHienTai" AS (
  SELECT 
    "MaSanPham", 
    "Thang",
    SUM("TongSoLuongBan") AS "SoLuongHienTai"
  FROM "DoanhSoToanBo"
  WHERE "Thang" = EXTRACT(MONTH FROM SYSDATE)
  GROUP BY "MaSanPham", "Thang"
),

"ThangTruoc" AS (
  SELECT 
    "MaSanPham", 
    "Thang",
    SUM("TongSoLuongBan") AS "SoLuongThangTruoc"
  FROM "DoanhSoToanBo"
  WHERE "Thang" = EXTRACT(MONTH FROM SYSDATE) - 1
  GROUP BY "MaSanPham", "Thang"
)

SELECT 
  tt."MaSanPham",
  tt."SoLuongThangTruoc",
  ht."SoLuongHienTai",
  tt."Thang" AS "ThangTruoc",
  ht."Thang" AS "ThangHienTai",
  ROUND(((ht."SoLuongHienTai" - tt."SoLuongThangTruoc") * 100.0) / NULLIF(tt."SoLuongThangTruoc", 0), 2) AS "TyLeGiam(%)"
FROM "ThangTruoc" tt
JOIN "ThangHienTai" ht ON tt."MaSanPham" = ht."MaSanPham"
WHERE ((ht."SoLuongHienTai" - tt."SoLuongThangTruoc") * 1.0) / NULLIF(tt."SoLuongThangTruoc", 0) <= -0.3
ORDER BY "TyLeGiam(%)";


/*
Query 8: Tại chi nhánh 1, với vai trò là GiamDoc, thống kê top 10 sản phẩm bán chạy nhất trong cả hệ thống (SUM số hóa đơn cả 3 chi nhánh)
*/

WITH "DSChiTietHoaDon" AS(
  SELECT cthd."MaSanPham", cthd."SoLuong" 
  FROM BTL1."ChiTietHoaDon" cthd

  UNION ALL 

  SELECT cthd."MaSanPham", cthd."SoLuong" 
  FROM BTL1."ChiTietHoaDon"@GiamDoc12Link cthd 
  
  UNION ALL 

  SELECT cthd."MaSanPham", cthd."SoLuong" 
  FROM BTL1."ChiTietHoaDon"@GiamDoc13Link cthd
)
SELECT 
  sp."MaSanPham",
  sp."TenSanPham",
  sp_hd."TongSLBanRa"
FROM 
  (SELECT
    ds."MaSanPham",
    SUM(ds."SoLuong") "TongSLBanRa"
  FROM "DSChiTietHoaDon" ds
  GROUP BY ds."MaSanPham"
  ORDER BY "TongSLBanRa" DESC
  FETCH FIRST 10 ROWS ONLY) sp_hd
  JOIN BTL1."SanPham" sp ON sp_hd."MaSanPham" = sp."MaSanPham";


/*
WITH "DSChiTietHoaDon" AS (
    SELECT "MaSanPham", "SoLuong" 
    FROM BTL1."ChiTietHoaDon"

    UNION ALL

    SELECT "MaSanPham", "SoLuong" 
    FROM BTL1."ChiTietHoaDon"@GiamDoc12Link

    UNION ALL

    SELECT "MaSanPham", "SoLuong" 
    FROM BTL1."ChiTietHoaDon"@GiamDoc13Link
),
"TopSanPham" AS (
    SELECT 
        "MaSanPham",
        SUM("SoLuong") AS "TongSLBanRa"
    FROM "DSChiTietHoaDon"
    GROUP BY "MaSanPham"
    ORDER BY "TongSLBanRa" DESC
    FETCH FIRST 10 ROWS ONLY
)
SELECT 
    sp."MaSanPham",
    sp."TenSanPham",
    ts."TongSLBanRa"
FROM "TopSanPham" ts
JOIN BTL1."SanPham" sp 
    ON ts."MaSanPham" = sp."MaSanPham"
ORDER BY ts."TongSLBanRa" DESC;
*/

/*
Query 9:
Tại chi nhánh 3, với vai trò là NhanVien
Cho biết tỉ lệ đơn hàng đã mua sản phẩm ‘chậu’ kèm với sản phẩm ‘cây’ trong cùng 1 đơn hàng ở từng chi nhánh 2 và chi nhánh 3
*/
SELECT * FROM (
  -- Chi nhánh 2:
  (
    SELECT 
      ( -- Calculate ratio for Chi nhánh 2
        SELECT COUNT(*) 
        FROM (
          SELECT CTHD."MaHoaDon"
          FROM BTL1."ChiTietHoaDon"@NhanVien32Link CTHD
          JOIN BTL1."SanPham" SP ON CTHD."MaSanPham" = SP."MaSanPham"
          GROUP BY CTHD."MaHoaDon"
          HAVING COUNT(DISTINCT SP."TheLoai") = 2
        )
      ) 
      / 
      (
        SELECT COUNT("MaHoaDon") 
        FROM BTL1."HoaDon"@NhanVien32Link
      ) AS TiLeDonHang,
      'Chi nhánh 2' AS TenChiNhanh
	  FROM dual
  )
  UNION ALL
  -- Chi nhánh 3:
  (
    SELECT 
      ( -- Calculate ratio for Chi nhánh 3
        SELECT COUNT(*) 
        FROM (
          SELECT CTHD."MaHoaDon"
          FROM BTL1."ChiTietHoaDon" CTHD
          JOIN BTL1."SanPham" SP ON CTHD."MaSanPham" = SP."MaSanPham"
          GROUP BY CTHD."MaHoaDon"
          HAVING COUNT(DISTINCT SP."TheLoai") = 2
        )
      ) 
      / 
      (
        SELECT COUNT("MaHoaDon") 
        FROM BTL1."HoaDon"
      ) AS TiLeDonHang,
      'Chi nhánh 3' AS TenChiNhanh
    FROM DUAL
  )
);

/*
Query 10:
Tại chi nhánh 3, với role là QuanLyKho
Cho biết các sản phẩm có số lượng tồn kho tại một chi nhánh chiếm >= 50% tổng tồn kho của sản phẩm đó trong tất cả chi nhánh
*/
WITH All_SanPham_QLKho AS (
	(SELECT * FROM BTL1."KhoSanPham_QLKho")
		UNION ALL
	(SELECT * FROM BTL1."KhoSanPham_QLKho"@QuanLyKho31Link)
		UNION ALL
	(SELECT * FROM BTL1."KhoSanPham_QLKho"@QuanLyKho32Link)
)

SELECT
	A1."MaSanPham",
	A1."MaChiNhanh",
	A1."SoLuong" "TonKho",
	(SELECT SUM("SoLuong") "TonKho"
	FROM  All_SanPham_QLKho A2 
	WHERE A1."MaSanPham" = A2."MaSanPham") "TonKhoToanHeThong"
FROM All_SanPham_QLKho A1
WHERE A1."SoLuong" * 100 / NULLIF(
	(SELECT SUM("SoLuong") 
	FROM  All_SanPham_QLKho A2 
	WHERE A1."MaSanPham" = A2."MaSanPham")
, 0) >= 50
ORDER BY A1."MaSanPham";

------------------------------------------------------------------------------------------------------------------------------------------

/* CHƯƠNG 4: Câu truy vấn chưa tối ưu ban đầu */
SELECT 
    SP."MaSanPham",
    SP."TenSanPham",
    SP."Gia",
    QLK."SoLuong" AS SoLuongTonKho,
    HD."NgayTao",
    NV."HoTen" AS TenNhanVien
FROM "ChiTietHoaDon" CTHD
JOIN "HoaDon" HD ON CTHD."MaHoaDon" = HD."MaHoaDon"
JOIN "NhanVien" NV ON HD."MaNhanVien" = NV."MaNhanVien"
JOIN "ChiNhanh" CN ON NV."MaChiNhanh" = CN."MaChiNhanh"
JOIN "SanPham" SP ON CTHD."MaSanPham" = SP."MaSanPham"
JOIN "KhoSanPham_QLKho" QLK ON SP."MaSanPham" = QLK."MaSanPham"
WHERE 
    NV."HoTen" LIKE '% Vỹ'
    AND QLK."SoLuong" > 20
    AND SP."Gia" BETWEEN 100000 AND 1000000
    AND CN."DiaChi" = 'Hà Nội'
    AND TRUNC(HD."NgayTao") = TO_DATE('01/01/2025', 'dd/mm/yyyy');

/* CHƯƠNG 4: EXPLAIN Câu truy vấn chưa tối ưu ban đầu */

-- Bật thống kê thời gian thực trong Oracle
ALTER SESSION SET statistics_level = ALL;

-- Thu thập dữ liệu thống kê cho câu truy vấn
SELECT /*+ GATHER_PLAN_STATISTICS */
    SP."MaSanPham",
    SP."TenSanPham",
    SP."Gia",
    QLK."SoLuong" AS SoLuongTonKho,
    HD."NgayTao",
    NV."HoTen" AS TenNhanVien
FROM "ChiTietHoaDon" CTHD
JOIN "HoaDon" HD ON CTHD."MaHoaDon" = HD."MaHoaDon"
JOIN "NhanVien" NV ON HD."MaNhanVien" = NV."MaNhanVien"
JOIN "ChiNhanh" CN ON NV."MaChiNhanh" = CN."MaChiNhanh"
JOIN "SanPham" SP ON CTHD."MaSanPham" = SP."MaSanPham"
JOIN "KhoSanPham_QLKho" QLK ON SP."MaSanPham" = QLK."MaSanPham"
WHERE 
    NV."HoTen" LIKE '% Vỹ'
    AND QLK."SoLuong" > 20
    AND SP."Gia" BETWEEN 100000 AND 1000000
    AND CN."DiaChi" = 'Hà Nội'
    AND TRUNC(HD."NgayTao") = TO_DATE('01/01/2025', 'dd/mm/yyyy');

-- Lấy SQL_ID của truy vấn vừa thực thi
SELECT sql_id, sql_text
FROM v$sql
WHERE sql_text LIKE '%SP."MaSanPham"%' AND sql_text NOT LIKE '%v$transaction%'
ORDER BY last_load_time DESC;

-- Explain câu truy vấn có SQL_ID vừa tìm được ở câu lệnh trên
SELECT * FROM TABLE(DBMS_XPLAN.display_cursor('df5njg2jzd50c', NULL, 'ALLSTATS LAST'));

--SELECT * FROM TABLE(DBMS_XPLAN.display_cursor(format=>'ALLSTATS LAST'));


/* CHƯƠNG 4: CÂU TRUY VẤN TỐI ƯU TRÊN CÁC MẢNH */

-- Thu thập dữ liệu thống kê cho câu truy vấn đã tối ưu trên các mảnh
SELECT /*+ GATHER_PLAN_STATISTICS */
  "BLOCK3"."MaSanPham",
  "BLOCK3"."TenSanPham",
  "BLOCK3"."Gia",
  "QLK1"."SoLuong",
  "BLOCK3"."NgayTao",
  "BLOCK3"."HoTen"
FROM (
  SELECT 
    "SP"."MaSanPham",
    "SP"."TenSanPham",
    "SP"."Gia",
    "BLOCK2"."HoTen",
    "BLOCK2"."NgayTao"
  FROM (
    SELECT 
      "CTHD1"."MaSanPham",
      "NV1"."HoTen",
      "HD1"."NgayTao"
    FROM (
      SELECT "NV"."MaNhanVien", "NV"."HoTen"
      FROM "NhanVien" "NV"
      WHERE "NV"."HoTen" LIKE '% Vỹ'
    ) "NV1"
    JOIN (
      SELECT "HD"."MaHoaDon", "HD"."MaNhanVien", "HD"."NgayTao"
      FROM "HoaDon" "HD"
      WHERE TRUNC("HD"."NgayTao") = TO_DATE('01/01/2025', 'DD/MM/YYYY')
    ) "HD1" ON "NV1"."MaNhanVien" = "HD1"."MaNhanVien"
    JOIN (
      SELECT "MaHoaDon", "MaSanPham"
      FROM "ChiTietHoaDon"
    ) "CTHD1" ON "CTHD1"."MaHoaDon" = "HD1"."MaHoaDon"
  ) "BLOCK2"
  JOIN (
    SELECT "MaSanPham", "TenSanPham", "Gia"
    FROM "SanPham"
    WHERE "Gia" BETWEEN 100000 AND 1000000
  ) "SP" ON "SP"."MaSanPham" = "BLOCK2"."MaSanPham"
) "BLOCK3"
JOIN (
  SELECT "MaSanPham", "SoLuong"
  FROM "KhoSanPham_QLKho"
  WHERE "SoLuong" > 20
) "QLK1" ON "BLOCK3"."MaSanPham" = "QLK1"."MaSanPham";

-- Tìm SQL_ID của câu truy vấn tối ưu vừa thực thi
SELECT sql_id, sql_text
FROM v$sql
WHERE sql_text LIKE '%"BLOCK3"."MaSanPham"%' AND sql_text NOT LIKE '%v$transaction%'
ORDER BY last_load_time DESC;

-- Chạy EXPLAIN câu truy vấn có SQL_ID vừa tìm được
SELECT * FROM TABLE(DBMS_XPLAN.display_cursor('8y4mz3am3zgjj', NULL, 'ALLSTATS LAST'));

----------------------------------------------
/*Dùng CTE: @ducminh làm vẫn đúng
WITH BLOCK_1 AS (
    SELECT NV_MINI."MaNhanVien", NV_MINI."HoTen", HD_MINI."MaHoaDon", HD_MINI."NgayTao"
    FROM (
        SELECT "MaNhanVien", "HoTen"
        FROM "NhanVien"
        WHERE "HoTen" LIKE '% Vỹ'
    ) NV_MINI
    JOIN (
        SELECT "MaNhanVien", "MaHoaDon", "NgayTao"
        FROM "HoaDon"
        WHERE TRUNC("NgayTao") = TO_DATE('01/01/2025', 'dd/mm/yyyy')
    ) HD_MINI
    ON NV_MINI."MaNhanVien" = HD_MINI."MaNhanVien"
),
BLOCK_2 AS (
    SELECT BLOCK_1_MINI."HoTen", BLOCK_1_MINI."MaHoaDon", BLOCK_1_MINI."NgayTao", CTHD_MINI."MaSanPham"
    FROM (
        SELECT "HoTen", "MaHoaDon", "NgayTao"
        FROM BLOCK_1
    ) BLOCK_1_MINI
    JOIN (
        SELECT "MaHoaDon", "MaSanPham"
        FROM "ChiTietHoaDon"
    ) CTHD_MINI
    ON CTHD_MINI."MaHoaDon" = BLOCK_1_MINI."MaHoaDon"
),
BLOCK_3 AS (
    SELECT BLOCK_2_MINI."MaSanPham", BLOCK_2_MINI."HoTen", BLOCK_2_MINI."NgayTao", SP_MINI."TenSanPham", SP_MINI."Gia"
    FROM (
        SELECT "MaSanPham", "HoTen", "NgayTao"
        FROM BLOCK_2
    ) BLOCK_2_MINI
    JOIN (
        SELECT "MaSanPham", "TenSanPham", "Gia"
        FROM "SanPham"
        WHERE "Gia" BETWEEN 100000 AND 1000000
    ) SP_MINI
    ON SP_MINI."MaSanPham" = BLOCK_2_MINI."MaSanPham"
),
BLOCK_4 AS (
    SELECT BLOCK_3_MINI."MaSanPham", BLOCK_3_MINI."TenSanPham", BLOCK_3_MINI."Gia", BLOCK_3_MINI."HoTen", BLOCK_3_MINI."NgayTao", QLK_MINI."SoLuong"
    FROM (
        SELECT "MaSanPham", "TenSanPham", "Gia", "HoTen", "NgayTao"
        FROM BLOCK_3
    ) BLOCK_3_MINI
    JOIN (
        SELECT "MaSanPham", "SoLuong"
        FROM "KhoSanPham_QLKho"
        WHERE "SoLuong" > 20
    ) QLK_MINI
    ON BLOCK_3_MINI."MaSanPham" = QLK_MINI."MaSanPham"
)
SELECT * FROM BLOCK_4;
*/

----------------------------------------------------------
/* --Câu này @congphan viết thì chạy bị bug
SELECT
  "MaSanPham",
  "TenSanPham",
  "Gia",
  "SoLuong",
  "NgayTao",
  "HoTen"
FROM(
  (SELECT BLOCK3."MaSanPham", BLOCK3."TenSanPham", BLOCK3."Gia", BLOCK3."HoTen", BLOCK3."NgayTao"
   FROM(

     SELECT BLOCK2."MaSanPham", BLOCK2."HoTen", BLOCK2."NgayTao"
     FROM(
      (SELECT NV1."HoTen", HD1."MaHoaDon", HD1."NgayTao"
       FROM (
        (SELECT "MaNhanVien", "HoTen"
         FROM "NhanVien"
         WHERE "HoTen" LIKE '% Vỹ') NV1
         JOIN
         (SELECT "MaNhanVien", "MaHoaDon", "NgayTao"
          FROM "HoaDon"
          WHERE TRUNC("NgayTao") = TO_DATE('01/01/2025', 'dd/mm/yyyy')
          ) HD1 ON NV1."MaNhanVien" = HD1."MaNhanVien"
        )
       ) BLOCK1
       JOIN 
       (SELECT "MaHoaDon", "MaSanPham"
        FROM "ChiTietHoaDon") CTHD1 ON CTHD1."MaHoaDon" = BLOCK1."MaHoaDon"
      ) BLOCK2
       JOIN
       (SELECT "MaSanPham", "TenSanPham", "Gia"
        FROM "SanPham"
        WHERE "Gia" BETWEEN 100000 AND 1000000
        ) SP ON SP."MaSanPham" = BLOCK2."MaSanPham"
   ) BLOCK3
  JOIN
   (SELECT "MaSanPham", "SoLuong"
    FROM "KhoSanPham_QLKho"
    WHERE "SoLuong" > 20
    ) QLK1 ON BLOCK3."MaSanPham" = QLK1."MaSanPham"
);
*/

------------------------------------------------------------------------------------------------------------------------------------------

/* CHƯƠNG 2 (TRIGGER, FUNCTION, PROCEDURE) */

--Procedure: Giả sử trường hợp là giám đốc sau khi nắm được tình hình hiệu suất làm việc của nhân viên và quyết định tăng lương cho nhân viên
CREATE OR REPLACE PROCEDURE TangLuongNhanVien(
  p_ma_nhan_vien IN NUMBER,
  p_luong IN NUMBER
)
AS
  dem NUMBER := 0;
  luong_hien_tai NUMBER;
BEGIN
  SELECT COUNT("MaNhanVien") INTO dem FROM BTL1."NhanVien" WHERE "MaNhanVien" = p_ma_nhan_vien;
  IF (dem = 1) THEN
    --Nhân viên thuộc Chi nhánh 1
    SELECT "Luong" INTO luong_hien_tai FROM BTL1."NhanVien" WHERE "MaNhanVien" = p_ma_nhan_vien;
    IF (p_luong > luong_hien_tai) THEN
      UPDATE BTL1."NhanVien"
      SET "Luong" = p_luong
      WHERE "MaNhanVien" = p_ma_nhan_vien;
    ELSE
      RAISE_APPLICATION_ERROR(-20000, 'Error: Lương cập nhật phải lớn hơn lương hiện tại của nhân viên!');
    END IF;
  ELSE
    SELECT COUNT("MaNhanVien") INTO dem FROM BTL1."NhanVien"@GiamDoc12Link WHERE "MaNhanVien" = p_ma_nhan_vien;
    IF (dem = 1) THEN
      --Nhân viên thuộc Chi nhánh 2
      SELECT "Luong" INTO luong_hien_tai FROM BTL1."NhanVien"@GiamDoc12Link WHERE "MaNhanVien" = p_ma_nhan_vien;
      IF (p_luong > luong_hien_tai) THEN
        UPDATE BTL1."NhanVien"@GiamDoc12Link
        SET "Luong" = p_luong
        WHERE "MaNhanVien" = p_ma_nhan_vien;
      ELSE
        RAISE_APPLICATION_ERROR(-20000, 'Error: Lương cập nhật phải lớn hơn lương hiện tại của nhân viên!');
      END IF;
    ELSE
      SELECT COUNT("MaNhanVien") INTO dem FROM BTL1."NhanVien"@GiamDoc13Link WHERE "MaNhanVien" = p_ma_nhan_vien;
      IF (dem = 1) THEN
        --Nhân viên thuộc Chi nhánh 3
        SELECT "Luong" INTO luong_hien_tai FROM BTL1."NhanVien"@GiamDoc13Link WHERE "MaNhanVien" = p_ma_nhan_vien;
        IF (p_luong > luong_hien_tai) THEN
          UPDATE BTL1."NhanVien"@GiamDoc13Link
          SET "Luong" = p_luong
          WHERE "MaNhanVien" = p_ma_nhan_vien;
        ELSE
          RAISE_APPLICATION_ERROR(-20000, 'Error: Lương cập nhật phải lớn hơn lương hiện tại của nhân viên!');
        END IF;
      ELSE
        RAISE_APPLICATION_ERROR(-20001, 'Error: Không tồn tại mã nhân viên trong hệ thống Plant Paradise!');
      END IF;
    END IF;
  END IF;

  DBMS_OUTPUT.PUT_LINE('Cập nhật lương thành công!');
  COMMIT;
END;

--Cấp quyền thực thi cho GiamDoc
GRANT EXECUTE ON TangLuongNhanVien TO GiamDoc;

SELECT * FROM BTL1."NhanVien" nv WHERE NV."MaNhanVien" = 113;

--Thực thi procedure
BEGIN
    BTL1.TangLuongNhanVien(113, 7000000);
END;
/


/*Trigger
Khi có thao tác đến chi tiết hoá đơn thì tiến hành cập nhật tồn kho và tổng số lượng đã bán của sản phẩm đó trong kho sản phẩm

                         Thêm      Xoá       Sửa
ChiTietHoaDon             +         -         +(SoLuong, MaSanPham)
KhoSanPham_QLKho          -         -         +(1)
KhoSanPham_QLBanHang      -         +(1)      +(TongSoLuongDaBan)

Chú thích:
- KhoSanPham_QLKho:
  + Thao tác sửa sẽ không ảnh hưởng trực tiếp đến ràng buộc toàn vẹn trong phát biểu này
  + Tuy nhiên, việc thay đổi số lượng tồn kho ảnh hưởng đến độ chính xác tồn kho thực tế nếu quản lý kho trong quá trình nhập liệu bị sai sót

- KhoSanPham_QLBanHang: Nếu bảng này bị xóa thì không thể cập nhật tổng số lượng sản phẩm đã bán khi thêm chi tiết hóa đơn
- Một khi đã xuất hoá đơn (nghĩa là có các chi tiết hoá đơn) thì không thể xoá được - theo thực tế, khi đã xuất hoá đơn giấy cho khách hàng rồi thì đâu có xoá cái đó được,
trừ khi xuất hoá đơn mới (ví dụ người dùng mua hàng và xuất hoá đơn rồi nhưng sau đó muốn trả lại một món hàng nào đó trong hoá đơn, thì xuất hoá đơn giấy mới)
*/


SELECT * FROM BTL1."ChiTietHoaDon"@NhanVien12Link WHERE "MaSanPham" = 'CCNPLT0307';
SELECT * FROM BTL1."KhoSanPham_QLKho"@QuanLyKho12Link where "MaSanPham" = 'CCNPLT0307';
SELECT * FROM BTL1."KhoSanPham_QLBanHang"@QuanLyKho12Link where "MaSanPham" = 'CCNPLT0307';


SELECT * FROM BTL1."ChiTietHoaDon" WHERE "MaSanPham" = 'CCNPLT0307'; --sôluong=32
SELECT * FROM BTL1."KhoSanPham_QLKho" where "MaSanPham" = 'CCNPLT0307'; --tonkho=22
SELECT * FROM BTL1."KhoSanPham_QLBanHang" where "MaSanPham" = 'CCNPLT0307';--soluongbanra=8933



SELECT * FROM BTL1."ChiTietHoaDon" where "MaSanPham" = 'CCNPLT0186'; --mahoadon=12 va soluong=1 --sửa thành 2
SELECT * FROM "KhoSanPham_QLKho" where "MaSanPham" = 'CCNPLT0186'; --soluongtonkho=97
SELECT * FROM BTL1."KhoSanPham_QLBanHang" where "MaSanPham" = 'CCNPLT0186'; --tongsldaban=8501


SELECT * FROM BTL1."ChiTietHoaDon" where "MaSanPham" = 'CCNPLT0317'; --mahoadon=3 va soluong=7 --sửa thành 8
SELECT * FROM "KhoSanPham_QLKho" where "MaSanPham" = 'CCNPLT0317'; --soluongtonkho=31
SELECT * FROM BTL1."KhoSanPham_QLBanHang" where "MaSanPham" = 'CCNPLT0317'; --tongsldaban=8987

SELECT * FROM BTL1."KhoSanPham_QLBanHang" where "MaSanPham" = 'CCNPLT0100';

SELECT *
FROM BTL1."ChiTietHoaDon"
WHERE "MaHoaDon" NOT IN (SELECT "MaHoaDon" FROM BTL1."ChiTietHoaDon" WHERE "MaSanPham" = 'CCNPLT0186');

DELETE FROM BTL1."ChiTietHoaDon" WHERE "MaHoaDon" = 9 AND "MaSanPham" = 'CCNPLT0307';

INSERT INTO BTL1."ChiTietHoaDon"@NhanVien12Link VALUES (2, 'CCNPLT0307', 7, 123000);
COMMIT;

ROLLBACK
COMMIT;

SELECT SUM("SoLuong") FROM BTL1."ChiTietHoaDon" WHERE "MaSanPham" = 'CCNPLT0186';
SELECT SUM("SoLuong") FROM BTL1."ChiTietHoaDon"@NhanVien12Link WHERE "MaSanPham" = 'CCNPLT0307';

UPDATE BTL1."KhoSanPham_QLBanHang"@QuanLyKho12Link
SET "TongSoLuongDaBan" = 8933
WHERE "MaSanPham" = 'CCNPLT0307';

UPDATE BTL1."KhoSanPham_QLBanHang"
SET "TongSoLuongDaBan" = 8501
WHERE "MaSanPham" = 'CCNPLT0186';


UPDATE BTL1."KhoSanPham_QLKho"
SET "SoLuong" = 22
WHERE "MaSanPham" = 'CCNPLT0307';

UPDATE BTL1."ChiTietHoaDon"
SET "SoLuong" = 32
WHERE "MaSanPham" = 'CCNPLT0307' AND "MaHoaDon" = 3;
COMMIT;

UPDATE BTL1."ChiTietHoaDon"@NhanVien12Link
SET "SoLuong" = 12
WHERE "MaSanPham" = 'CCNPLT0307' AND "MaHoaDon" = 42;

--------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER ChiTietHoaDon_Change_Trigger
FOR INSERT OR UPDATE ON BTL1."ChiTietHoaDon"
COMPOUND TRIGGER

  -- Bảng tạm để lưu các thay đổi
  TYPE MaSanPhamTab IS TABLE OF BTL1."ChiTietHoaDon"."MaSanPham"%TYPE INDEX BY PLS_INTEGER;
  TYPE SoLuongTab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  TYPE OldSoLuongTab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  
  maSanPhamList MaSanPhamTab;
  soLuongList SoLuongTab;
  oldSoLuongList OldSoLuongTab;

  BEFORE STATEMENT IS
  BEGIN
    maSanPhamList.DELETE;
    soLuongList.DELETE;
    oldSoLuongList.DELETE;
  END BEFORE STATEMENT;

  AFTER EACH ROW IS
  BEGIN
    maSanPhamList(maSanPhamList.COUNT + 1) := :NEW."MaSanPham";
    soLuongList(soLuongList.COUNT + 1) := :NEW."SoLuong";
    IF UPDATING THEN
      oldSoLuongList(oldSoLuongList.COUNT + 1) := :OLD."SoLuong";
    ELSE
      oldSoLuongList(oldSoLuongList.COUNT + 1) := 0;
    END IF;
  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    FOR i IN 1..maSanPhamList.COUNT LOOP
      DECLARE
        maSanPham VARCHAR2(50);
        soLuong NUMBER;
        oldSoLuong NUMBER;
      BEGIN
        maSanPham := maSanPhamList(i);
        soLuong := soLuongList(i);
        oldSoLuong := oldSoLuongList(i);

        IF UPDATING THEN
          -- Cộng tồn kho cũ
          UPDATE "KhoSanPham_QLKho"
          SET "SoLuong" = "SoLuong" + oldSoLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;

          -- Giảm tồn kho mới
          UPDATE "KhoSanPham_QLKho"
          SET "SoLuong" = "SoLuong" - soLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;

          -- Cập nhật tổng bán
          UPDATE "KhoSanPham_QLBanHang"
          SET "TongSoLuongDaBan" = "TongSoLuongDaBan" + (soLuong - oldSoLuong), "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;

        ELSIF INSERTING THEN
          -- Giảm tồn kho
          UPDATE "KhoSanPham_QLKho"
          SET "SoLuong" = "SoLuong" - soLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;

          -- Cập nhật tổng bán
          UPDATE "KhoSanPham_QLBanHang"
          SET "TongSoLuongDaBan" = "TongSoLuongDaBan" + soLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;
        END IF;

      END;
    END LOOP;
  END AFTER STATEMENT;

END ChiTietHoaDon_Change_Trigger;
/
------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER Sua_TongSLDaBanCuaSanPham_Trigger
FOR UPDATE ON "KhoSanPham_QLBanHang"
COMPOUND TRIGGER

  TYPE MaSanPhamTab IS TABLE OF "KhoSanPham_QLBanHang"."MaSanPham"%TYPE INDEX BY PLS_INTEGER;
  maSanPhamList MaSanPhamTab;

  AFTER EACH ROW IS
  BEGIN
    maSanPhamList(maSanPhamList.COUNT + 1) := :NEW."MaSanPham";
  END AFTER EACH ROW;

  BEFORE STATEMENT IS
    tongSoLuongDaBan NUMBER;
    tongSoLuongChiTiet NUMBER;
  BEGIN

    FOR i IN 1..maSanPhamList.COUNT LOOP
      -- Lấy tổng số lượng đã bán từ KhoSanPham_QLBanHang
      SELECT "TongSoLuongDaBan"
      INTO tongSoLuongDaBan
      FROM "KhoSanPham_QLBanHang"
      WHERE "MaSanPham" = maSanPhamList(i);

      -- Lấy tổng số lượng đã bán thực tế từ ChiTietHoaDon
      SELECT NVL(SUM("SoLuong"), 0)
      INTO tongSoLuongChiTiet
      FROM "ChiTietHoaDon"
      WHERE "MaSanPham" = maSanPhamList(i);

      --So sánh
      IF tongSoLuongDaBan <> tongSoLuongChiTiet THEN
          RAISE_APPLICATION_ERROR(-20000, 'Lỗi: Cập nhật thất bại!');
      END IF;
    END LOOP;
  END BEFORE STATEMENT;
END Sua_TongSLDaBanCuaSanPham_Trigger;
/
-----------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER KiemTra_TongSLDaBan_AfterStatement
AFTER UPDATE ON "KhoSanPham_QLBanHang"
FOR EACH ROW 
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
  tongSoLuongChiTiet NUMBER;

BEGIN

    -- Lấy tổng số lượng thực tế từ ChiTietHoaDon
    SELECT NVL(SUM("SoLuong"), 0)
    INTO tongSoLuongChiTiet
    FROM "ChiTietHoaDon"
    WHERE "MaSanPham" = :NEW."MaSanPham";

    -- So sánh
    IF :NEW."TongSoLuongDaBan" <> tongSoLuongChiTiet THEN
      RAISE_APPLICATION_ERROR(
            -20001,
            'Lỗi: Tổng số lượng bán không khớp cho sản phẩm ' ||
            :NEW."MaSanPham" ||
            ' | TongSoLuongDaBan=' || :NEW."TongSoLuongDaBan" ||
            ' | TongSoLuongChiTiet=' || tongSoLuongChiTiet
          );
    END IF;
END;
/
DROP TRIGGER BTL1.KIEMTRA_TONGSLDABAN_AFTERSTATEMENT;

--Function
/*
-Chức năng hàm: tính số hóa đơn đã bán của một nhân viên theo tháng
-Đối tượng sử dụng: người dùng với role GiamDoc ở chi nhánh 1
-Dữ liệu đầu vào:
  + Mã nhân viên của một nhân viên bất kỳ trong hệ thống
  + Tháng (từ 1 đến 12)
  + Năm
-Kết quả đầu ra: Số hóa đơn đã bán được của một nhân viên trong tháng đã chỉ định.
*/

CREATE OR REPLACE FUNCTION fn_dem_hoa_don_nhan_vien(
    p_ma_nhan_vien IN NUMBER,
    p_thang IN NUMBER,
    p_nam IN NUMBER
) RETURN NUMBER
IS
    v_dem_hoa_don NUMBER := 0;
    v_link_name VARCHAR2(20);
    v_ma_chi_nhanh NUMBER;
    v_sql VARCHAR2(1000);
BEGIN
    -- Validate month input
    IF p_thang < 1 OR p_thang > 12 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Tháng không hợp lệ: Phải từ 1 đến 12');
    END IF;

    -- Check if employee exists and get MaChiNhanh
    BEGIN
        SELECT "MaChiNhanh"
        INTO v_ma_chi_nhanh
        FROM (
            SELECT "MaNhanVien", "MaChiNhanh" FROM BTL1."NhanVien"
            --UNION ALL
            --SELECT "MaNhanVien", "MaChiNhanh" FROM BTL1."NhanVien"@GiamDocLink12
            --UNION ALL
            --SELECT "MaNhanVien", "MaChiNhanh" FROM BTL1."NhanVien"@GiamDocLink13
        )
        WHERE "MaNhanVien" = p_ma_nhan_vien;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20003, 'Không tìm thấy nhân viên');
        WHEN TOO_MANY_ROWS THEN
            RAISE_APPLICATION_ERROR(-20004, 'Nhiều nhân viên trùng mã nhân viên');
    END;

    -- Assign database link based on MaChiNhanh
    IF v_ma_chi_nhanh = 1 THEN
        v_link_name := ''; -- Local query for Hà Nội
    ELSIF v_ma_chi_nhanh = 2 THEN
        v_link_name := 'GiamDoc12Link'; -- Đà Nẵng
    ELSIF v_ma_chi_nhanh = 3 THEN
        v_link_name := 'GiamDoc13Link'; -- Hồ Chí Minh
    ELSE
        RAISE_APPLICATION_ERROR(-20005, 'Chi nhánh không hợp lệ');
    END IF;

    -- Build dynamic SQL
    IF v_link_name IS NULL OR v_link_name = '' THEN
        v_sql := '
            SELECT COUNT("MaHoaDon")
            FROM BTL1."HoaDon"
            WHERE "MaNhanVien" = :1 AND
                  EXTRACT(MONTH FROM "NgayTao") = :2 AND
                  EXTRACT(YEAR FROM "NgayTao") = :3';
    ELSE
        v_sql := '
            SELECT COUNT("MaHoaDon")
            FROM BTL1."HoaDon"@' || DBMS_ASSERT.SQL_OBJECT_NAME(v_link_name) || '
            WHERE "MaNhanVien" = :1
                AND EXTRACT(MONTH FROM "NgayTao") = :2
                AND EXTRACT(YEAR FROM "NgayTao") = :3';
    END IF;

    -- Execute dynamic SQL
    EXECUTE IMMEDIATE v_sql
    INTO v_dem_hoa_don
    USING p_ma_nhan_vien, p_thang, p_nam;

    RETURN v_dem_hoa_don;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
    WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20001, 'Có lỗi xảy ra khi đếm số hóa đơn của nhân viên: ' || SQLERRM);
END fn_dem_hoa_don_nhan_vien;

GRANT EXECUTE ON fn_dem_hoa_don_nhan_vien TO GiamDoc;


------------------------------------------------------------------------------------------------------------------------------------------

/* CHƯƠNG 3: ĐIỀU KHIỂN TRUY XUẤT ĐỒNG THỜI */

-------------Phantom read----------------
COMMIT;
ALTER SESSION SET ISOLATION_LEVEL = READ COMMITTED;
ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE;
@hienphan
SELECT * FROM BTL1."KhoSanPham_QLKho" where "SoLuong" = 100;
UNION
SELECT * FROM BTL1."KhoSanPham_QLKho"@QuanLyKho21Link where "SoLuong" = 100;

@congphan
INSERT INTO "KhoSanPham_QLKho"
VALUES ('CCNPLT2222', 1, 100, TO_DATE('05/25/2025 12:00:00 AM', 'MM/DD/YYYY HH:MI:SS AM'));
COMMIT;

-------------Unrepatable read--------------
COMMIT;
ALTER SESSION SET ISOLATION_LEVEL = READ COMMITTED;
ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE;
@hienphan
SELECT * FROM "KhoSanPham_QLKho" where "SoLuong" >= 100;
UNION
SELECT * FROM "KhoSanPham_QLKho"@QuanLyKho21Link where "SoLuong" >= 100;

@congphan
UPDATE "KhoSanPham_QLKho"
SET "SoLuong" = 999
WHERE "MaSanPham" = 'CCNPLT6666';
COMMIT;


-----------------Lost update---------------
COMMIT;
ALTER SESSION SET ISOLATION_LEVEL = READ COMMITTED;
ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE;
@congphan
SELECT * FROM "KhoSanPham_QLBanHang" WHERE "MaSanPham" = 'CCNPLT0139';

UPDATE "KhoSanPham_QLBanHang"
SET "TinhTrang" = 'Còn hàng'
WHERE "MaSanPham" = 'CCNPLT0139';
COMMIT;


@hienphan
UPDATE "KhoSanPham_QLBanHang"@QuanLyKho21Link
SET "TinhTrang" = "Còn hàng"
WHERE "MaSanPham" = 'CCNPLT0139';
COMMIT;


------------------Deadlock-----------------
ALTER SESSION SET ISOLATION_LEVEL = READ COMMITTED;
ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE;
@congphan
UPDATE "KhoSanPham_QLBanHang"
SET "TongSoLuongDanhGia" = 77
WHERE "MaSanPham" = 'CCNPLT0285';
COMMIT;

UPDATE "KhoSanPham_QLBanHang"
SET "TongSoLuongDanhGia" = 888
WHERE "MaSanPham" = 'CCNPLT0300';
COMMIT;
ROLLBACK;
@hienphan
UPDATE "KhoSanPham_QLBanHang"@QuanLyKho21Link
SET "TongSoLuongDanhGia" = 999
WHERE "MaSanPham" = 'CCNPLT0300';
COMMIT;

UPDATE "KhoSanPham_QLBanHang"@QuanLyKho21Link
SET "TongSoLuongDanhGia" = 66
WHERE "MaSanPham" = 'CCNPLT0285';
COMMIT;

SELECT * FROM "KhoSanPham_QLBanHang" kspqh WHERE "MaSanPham" = 'CCNPLT0300';--108
SELECT * FROM "KhoSanPham_QLBanHang" kspqh WHERE "MaSanPham" = 'CCNPLT0285';--50


--Doanh thu của mỗi sản phẩm tại mỗi chi nhánh, theo từng quý và từng năm
--doanh_thu_sp_quy_cn
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
  KQL."MaChiNhanh", SP."MaSanPham", Nam, Quy;

--Doanh thu của mỗi sản phẩm tại mỗi chi nhánh, theo từng quý và từng năm
--doanh_thu_sp_quy_cn
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
  KQL."MaChiNhanh", SP."MaSanPham", Nam, Quy;

/*Schema ở Cassandra*/
CREATE TABLE doanh_thu_sp_quy_cn (
  ma_chi_nhanh int,
  ma_san_pham text,
  nam int,
  quy int,
  tong_doanh_thu bigint,
  PRIMARY KEY ((ma_chi_nhanh, ma_san_pham), nam, quy)
) WITH CLUSTERING ORDER BY (nam ASC, quy ASC);

--Doanh thu theo tháng của mỗi nhân viên ở từng chi nhánh
--doanh_thu_thang_nv_cn
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
  NV."MaChiNhanh", NV."MaNhanVien", Nam, Thang;


-- Explain plan 
/* Kế hoạch dự định chạy của truy vấn thôi chứ lúc chạy thật sự thì có thể khác kế hoạch dự định này
EXPLAIN PLAN FOR
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
FROM BTL1."ChiTietHoaDon"@NhanVien12Link cthd
  JOIN BTL1."HoaDon"@NhanVien12Link hd ON cthd."MaHoaDon" = hd."MaHoaDon"
WHERE hd."MaKhachHang" IN (34, 38)
ORDER BY hd."MaHoaDon", hd."NgayTao" DESC;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
*/

-- Đo thời gian thực hiện 
DECLARE
  v_sql        CLOB;
  v_start_time TIMESTAMP;
  v_end_time   TIMESTAMP;
  v_dummy      NUMBER;
BEGIN
  v_sql := '
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
    FROM BTL1."ChiTietHoaDon"@NhanVien12Link cthd 
      JOIN BTL1."HoaDon"@NhanVien12Link hd ON cthd."MaHoaDon" = hd."MaHoaDon"
    WHERE hd."MaKhachHang" IN (34, 38)
    ORDER BY hd."MaHoaDon", hd."NgayTao" DESC';

  v_start_time := SYSTIMESTAMP;

  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM (' || v_sql || ')' INTO v_dummy;

  v_end_time := SYSTIMESTAMP;

  DBMS_OUTPUT.PUT_LINE('Tổng thời gian thực thi câu truy vấn: ' || TO_CHAR(v_end_time - v_start_time));
  DBMS_OUTPUT.PUT_LINE('Số dòng: ' || v_dummy);
END;
/

------------test hiệu suất giữa truy vấn ở Oracle so với ở Cassandra-----------------------
SELECT /*+ GATHER_PLAN_STATISTICS */
  hd."MaKhachHang",
  cthd."MaHoaDon",
   cthd."MaSanPham",
  cthd."SoLuong",
  cthd."ThanhTien",
  hd."TongTien",
  hd."NgayTao",
  hd."PhuongThucThanhToan",
  hd."MaNhanVien"
FROM BTL1."ChiTietHoaDon"@NhanVien12Link cthd
  JOIN BTL1."HoaDon"@NhanVien12Link hd ON cthd."MaHoaDon" = hd."MaHoaDon"
WHERE hd."MaKhachHang" IN (34, 38) 
ORDER BY hd."MaHoaDon", hd."NgayTao" DESC;


-- Tìm SQL_ID của câu truy vấn tối ưu vừa thực thi
SELECT sql_id, sql_text
FROM v$sql
WHERE sql_text LIKE '%hd."MaKhachHang"%' AND sql_text NOT LIKE '%v$transaction%'
ORDER BY last_load_time DESC;

-- Chạy EXPLAIN câu truy vấn có SQL_ID vừa tìm được
SELECT * FROM TABLE(DBMS_XPLAN.display_cursor('6atn9j106td21', NULL, 'ALLSTATS LAST'));

--------------------------------
DECLARE
  v_sql        CLOB;
  v_start_time TIMESTAMP;
  v_end_time   TIMESTAMP;
  v_dummy      NUMBER;
BEGIN
  v_sql := '
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
  FROM BTL1."ChiTietHoaDon"@NhanVien12Link cthd
    JOIN BTL1."HoaDon"@NhanVien12Link hd ON cthd."MaHoaDon" = hd."MaHoaDon"
  WHERE hd."MaKhachHang" IN (34, 38) 
  ORDER BY hd."MaHoaDon", hd."NgayTao" DESC
  ';

  v_start_time := SYSTIMESTAMP;

  EXECUTE IMMEDIATE 'SELECT COUNT(DISTINCT "MaHoaDon") FROM (' || v_sql || ') t' INTO v_dummy;

  v_end_time := SYSTIMESTAMP;

  DBMS_OUTPUT.PUT_LINE('Tổng thời gian thực thi câu truy vấn: ' || TO_CHAR(v_end_time - v_start_time));
  DBMS_OUTPUT.PUT_LINE('Câu truy vấn trả về số dòng: ' || v_dummy);
END;


-------------------------------------------------------------
CREATE OR REPLACE TRIGGER ChiTietHoaDon_Change_Trigger
FOR INSERT OR UPDATE ON BTL1."ChiTietHoaDon"
COMPOUND TRIGGER

  -- Bảng tạm để lưu các thay đổi
  TYPE MaSanPhamTab IS TABLE OF BTL1."ChiTietHoaDon"."MaSanPham"%TYPE INDEX BY PLS_INTEGER;
  TYPE SoLuongTab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  TYPE OldSoLuongTab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

  maSanPhamList MaSanPhamTab;
  soLuongList SoLuongTab;
  oldSoLuongList OldSoLuongTab;

  BEFORE STATEMENT IS
  BEGIN
    maSanPhamList.DELETE;
    soLuongList.DELETE;
    oldSoLuongList.DELETE;
  END BEFORE STATEMENT;

  AFTER EACH ROW IS
  BEGIN
    maSanPhamList(maSanPhamList.COUNT + 1) := :NEW."MaSanPham";
    soLuongList(soLuongList.COUNT + 1) := :NEW."SoLuong";
    IF UPDATING THEN
      oldSoLuongList(oldSoLuongList.COUNT + 1) := :OLD."SoLuong";
    ELSE
      oldSoLuongList(oldSoLuongList.COUNT + 1) := 0;
    END IF;
  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    FOR i IN 1..maSanPhamList.COUNT LOOP
      DECLARE
        maSanPham VARCHAR2(50);
        soLuong NUMBER;
        oldSoLuong NUMBER;
      BEGIN
        maSanPham := maSanPhamList(i);
        soLuong := soLuongList(i);
        oldSoLuong := oldSoLuongList(i);

        IF UPDATING THEN
          -- Cộng tồn kho cũ
          UPDATE "KhoSanPham_QLKho"
          SET "SoLuong" = "SoLuong" + oldSoLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;

          -- Giảm tồn kho mới
          UPDATE "KhoSanPham_QLKho"
          SET "SoLuong" = "SoLuong" - soLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;

          -- Cập nhật tổng bán
          UPDATE "KhoSanPham_QLBanHang"
          SET "TongSoLuongDaBan" = "TongSoLuongDaBan" - oldSoLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;

          UPDATE "KhoSanPham_QLBanHang"
          SET "TongSoLuongDaBan" = "TongSoLuongDaBan" + soLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;

        ELSIF INSERTING THEN
          -- Giảm tồn kho
          UPDATE "KhoSanPham_QLKho"
          SET "SoLuong" = "SoLuong" - soLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;

          -- Cập nhật tổng bán
          UPDATE "KhoSanPham_QLBanHang"
          SET "TongSoLuongDaBan" = "TongSoLuongDaBan" + soLuong, "NgayCapNhat" = SYSDATE
          WHERE "MaSanPham" = maSanPham;
        END IF;
      END;
    END LOOP;
  END AFTER STATEMENT;

END ChiTietHoaDon_Change_Trigger;
/

CREATE OR REPLACE TRIGGER Sua_TongSLDaBanCuaSanPham_Trigger
FOR UPDATE ON "KhoSanPham_QLBanHang"
COMPOUND TRIGGER

  TYPE MaSanPhamTab IS TABLE OF "KhoSanPham_QLBanHang"."MaSanPham"%TYPE INDEX BY PLS_INTEGER;
  maSanPhamList MaSanPhamTab;

  AFTER EACH ROW IS
  BEGIN
    maSanPhamList(maSanPhamList.COUNT + 1) := :NEW."MaSanPham";
  END AFTER EACH ROW;

  BEFORE STATEMENT IS
    tongSoLuongDaBan NUMBER;
    tongSoLuongChiTiet NUMBER;
  BEGIN

    FOR i IN 1..maSanPhamList.COUNT LOOP
      -- Lấy tổng số lượng đã bán từ KhoSanPham_QLBanHang
      SELECT "TongSoLuongDaBan"
      INTO tongSoLuongDaBan
      FROM "KhoSanPham_QLBanHang"
      WHERE "MaSanPham" = maSanPhamList(i);

      -- Lấy tổng số lượng đã bán thực tế từ ChiTietHoaDon
      SELECT NVL(SUM("SoLuong"), 0)
      INTO tongSoLuongChiTiet
      FROM "ChiTietHoaDon"
      WHERE "MaSanPham" = maSanPhamList(i);

      --So sánh
      IF tongSoLuongDaBan <> tongSoLuongChiTiet THEN
          RAISE_APPLICATION_ERROR(-20000, 'Lỗi: Cập nhật thất bại!');
      END IF;
    END LOOP;
  END BEFORE STATEMENT;

END Sua_TongSLDaBanCuaSanPham_Trigger;
/

CREATE OR REPLACE TRIGGER KiemTra_TongSLDaBan_AfterStatement
AFTER UPDATE ON "KhoSanPham_QLBanHang"
FOR EACH ROW 
DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
  tongSoLuongChiTiet NUMBER;

BEGIN

    -- Lấy tổng số lượng thực tế từ ChiTietHoaDon
    SELECT NVL(SUM("SoLuong"), 0)
    INTO tongSoLuongChiTiet
    FROM "ChiTietHoaDon"
    WHERE "MaSanPham" = :NEW."MaSanPham";

    -- So sánh
    IF :NEW."TongSoLuongDaBan" <> tongSoLuongChiTiet THEN
      RAISE_APPLICATION_ERROR(-20001, 'Lỗi: Tổng số lượng bán không khớp cho sản phẩm ' || :NEW."MaSanPham");
    END IF;
END;
/

DROP TRIGGER BTL1.CHITIETHOADON_CHANGE_TRIGGER;
DROP TRIGGER BTL1.KIEMTRA_TONGSLDABAN_AFTERSTATEMENT;
DROP TRIGGER BTL1.SUA_TONGSLDABANCUASANPHAM_TRIGGER;
COMMIT;



UPDATE BTL1."KhoSanPham_QLBanHang"@QuanLyKho13Link
SET "TongSoLuongDaBan" = 9000
WHERE "MaSanPham" = 'CCNPLT0307';
