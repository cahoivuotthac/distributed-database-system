**Tên môn học**: Cơ sở dữ liệu phân tán

**Mã lớp**: IS211.P21
## Project Title: Distributed Database Design on Oracle RDBMS
  
  <img width="1276" height="639" alt="image" src="https://github.com/user-attachments/assets/641cb496-50c4-4457-a98b-3877fd7e6bbc" />

### Thiết kế chiến lược phân mảnh
  - Quan hệ CHINHANH là **phân mảnh ngang chính** theo địa chỉ
  - Quan hệ NHANVIEN, HOADON, CHITIETHOADON là **phân mảnh ngang dẫn xuất**
  - Quan hệ KHOSANPHAM được **phân mảnh hỗn hợp** thành các quan hệ  KHOSANPHAM_QLKHO và KHOSANPHAM_QLBANHANG
  - Quan hệ KHACHHANG, SANPHAM, DANHMUC_SANPHAM, THUOCTINH_SANPHAM **được nhân bản tại tất cả các chi nhánh**

### Distributed database with role-based access control
- Máy 1:
  
  <img width="613" height="728" alt="image" src="https://github.com/user-attachments/assets/2b85a7a7-ac71-440f-a663-2c7775154c59" />

- Máy 2:

  <img width="609" height="486" alt="image" src="https://github.com/user-attachments/assets/e5c6d126-772b-4b44-abfc-43a9def491f5" />

- Máy 3:

  <img width="606" height="482" alt="image" src="https://github.com/user-attachments/assets/01ea0b15-6ef1-4404-a1dc-bb3e15689bf1" />


## Project Title: Distributed Database Design on NoSQL
### Business requirements
  
### Data Flow Diagram
  <img width="1128" height="434" alt="image" src="https://github.com/user-attachments/assets/c3f91257-0aff-420a-918d-69283d6aa2bb" />

  
  > Data Quality as a first-class citizen 
