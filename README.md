- **Tên môn học**: Cơ sở dữ liệu phân tán
- **Mã lớp**: IS211.P21
- **BTL1**: THIẾT KẾ CƠ SỞ DỮ LIỆU PHÂN TÁN TRÊN HỆ QUẢN TRỊ CƠ SỞ DỮ LIỆU QUAN HỆ ORACLE
  
  <img width="1276" height="639" alt="image" src="https://github.com/user-attachments/assets/641cb496-50c4-4457-a98b-3877fd7e6bbc" />

  Thiết kế chiến lược phân mảnh:
  - Quan hệ CHINHANH là **phân mảnh ngang chính** theo địa chỉ
    
    <img width="362" height="82" alt="image" src="https://github.com/user-attachments/assets/5600899a-ffc3-49cb-b11b-cc470ba913f6" />

  - Quan hệ NHANVIEN, HOADON, CHITIETHOADON là **phân mảnh ngang dẫn xuất**
    
    <img width="421" height="298" alt="image" src="https://github.com/user-attachments/assets/e8a8b66b-e073-44c7-aded-e02b8355b059" />

  - Quan hệ KHOSANPHAM được **phân mảnh hỗn hợp** thành các quan hệ  KHOSANPHAM_QLKHO và KHOSANPHAM_QLBANHANG
    
    <img width="487" height="469" alt="image" src="https://github.com/user-attachments/assets/9e30021c-54f9-4e4b-b300-666844fc87a3" />


    
- **BTL2**: CƠ CHẾ PHÂN TÁN TRONG HỆ QUẢN TRỊ CASSANDRA
  
  <img width="664" height="318" alt="image" src="https://github.com/user-attachments/assets/23207284-1f9c-43b8-a870-ed9bf51b2c81" />
