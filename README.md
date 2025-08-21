- **Tên môn học**: Cơ sở dữ liệu phân tán
- **Mã lớp**: IS211.P21
- **BTL1**: THIẾT KẾ CƠ SỞ DỮ LIỆU PHÂN TÁN TRÊN HỆ QUẢN TRỊ CƠ SỞ DỮ LIỆU QUAN HỆ ORACLE
  
  <img width="1276" height="639" alt="image" src="https://github.com/user-attachments/assets/641cb496-50c4-4457-a98b-3877fd7e6bbc" />

  Thiết kế chiến lược phân mảnh:
  - Quan hệ CHINHANH là **phân mảnh ngang chính** theo địa chỉ
  - Quan hệ NHANVIEN, HOADON, CHITIETHOADON là **phân mảnh ngang dẫn xuất**
  - Quan hệ KHOSANPHAM được **phân mảnh hỗn hợp** thành các quan hệ  KHOSANPHAM_QLKHO và KHOSANPHAM_QLBANHANG
  - Quan hệ KHACHHANG, SANPHAM, DANHMUC_SANPHAM, THUOCTINH_SANPHAM **được nhân bản tại tất cả các chi nhánh**

- **BTL2**: CƠ CHẾ PHÂN TÁN TRONG HỆ QUẢN TRỊ CASSANDRA

  1. Business requirements:
  
  2. Data Flow Diagram: 
  <img width="1128" height="434" alt="image" src="https://github.com/user-attachments/assets/c3f91257-0aff-420a-918d-69283d6aa2bb" />

  
  > Data Quality as a first-class citizen 
