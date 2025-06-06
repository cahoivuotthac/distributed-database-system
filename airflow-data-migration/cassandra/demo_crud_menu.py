from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys
from datetime import datetime, date

class CassandraCRUD:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.connect()
    
    def connect(self):
        """Connect to Cassandra cluster"""
        try:
            self.cluster = Cluster(['127.0.0.1'])
            self.session = self.cluster.connect()
            self.session.execute("USE BTL2_data")
            print("✅ Connected to Cassandra successfully!")
        except Exception as e:
            print(f"❌ Error connecting to Cassandra: {e}")
            sys.exit(1)
    
    def disconnect(self):
        """Disconnect from Cassandra"""
        if self.cluster:
            self.cluster.shutdown()
            print("🔌 Disconnected from Cassandra")
    
    def display_main_menu(self):
        """Display main CRUD operations menu"""
        print("\n" + "="*50)
        print("🗄️  CASSANDRA CRUD OPERATIONS MENU")
        print("="*50)
        print("1. Chi tiết hóa đơn theo mã khách hàng")
        print("2. Doanh thu mỗi ngày theo mã chi nhánh")
        print("3. Kho sản phẩm theo mã chi nhánh")
        print("4. Số lượng khách hàng mỗi ngày theo mã chi nhánh")
        print("5. Doanh thu sản phẩm theo quý chi nhánh")
        print("6. Doanh thu tháng nhân viên chi nhánh")
        print("0. Exit")
        print("="*50)
    
    def display_crud_menu(self, table_name):
        """Display CRUD operations for a specific table"""
        print(f"\nCRUD Operations for {table_name}")
        print("-"*40)
        print("1. CREATE (Insert new record)")
        print("2. READ (View records)")
        print("3. UPDATE (Modify record)")
        print("4. DELETE (Remove record)")
        print("0. Back to main menu")
        print("-"*40)
    
    def insert_chi_tiet_hoa_don(self):
        """Insert record into chi_tiet_hoa_don_theo_ma_kh table"""
        print("\n➕ Insert new Chi tiết hóa đơn:")
        try:
            ma_khach_hang = int(input("Mã khách hàng: "))
            ma_hoa_don = int(input("Mã hóa đơn: "))
            ma_san_pham = input("Mã sản phẩm: ")
            so_luong = int(input("Số lượng: "))
            thanh_tien = int(input("Thành tiền: "))
            tong_tien = int(input("Tổng tiền: "))
            ngay_tao = input("Ngày tạo (YYYY-MM-DD HH:MM:SS): ")
            phuong_thuc_thanh_toan = input("Phương thức thanh toán: ")
            ma_nhan_vien = int(input("Mã nhân viên: "))
            
            query = """
            INSERT INTO chi_tiet_hoa_don_theo_ma_kh 
            (ma_khach_hang, ma_hoa_don, ma_san_pham, so_luong, thanh_tien, 
             tong_tien, ngay_tao, phuong_thuc_thanh_toan, ma_nhan_vien)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.session.execute(query, (ma_khach_hang, ma_hoa_don, ma_san_pham, 
                                       so_luong, thanh_tien, tong_tien, ngay_tao, 
                                       phuong_thuc_thanh_toan, ma_nhan_vien))
            print("✅ Record inserted successfully!")
        except Exception as e:
            print(f"❌ Error inserting record: {e}")
    
    def insert_kho_sp(self):
        """Insert record into kho_sp_theo_ma_cn table"""
        print("\n➕ Insert new Kho sản phẩm:")
        try:
            ma_chi_nhanh = int(input("Mã chi nhánh: "))
            ma_san_pham = input("Mã sản phẩm: ")
            ten_san_pham = input("Tên sản phẩm: ")
            tinh_trang = input("Tình trạng: ")
            tong_so_luong_danh_gia = int(input("Tổng số lượng đánh giá: "))
            tong_so_luong_da_ban = int(input("Tổng số lượng đã bán: "))
            tong_so_luong_ton_kho = int(input("Tổng số lượng tồn kho: "))
            
            query = """
            INSERT INTO kho_sp_theo_ma_cn 
            (ma_chi_nhanh, ma_san_pham, ten_san_pham, tinh_trang, 
             tong_so_luong_danh_gia, tong_so_luong_da_ban, tong_so_luong_ton_kho)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            self.session.execute(query, (ma_chi_nhanh, ma_san_pham, ten_san_pham, 
                                       tinh_trang, tong_so_luong_danh_gia, 
                                       tong_so_luong_da_ban, tong_so_luong_ton_kho))
            print("✅ Record inserted successfully!")
        except Exception as e:
            print(f"❌ Error inserting record: {e}")
    
    def read_records(self, table_name):
        """Read and display records from a table"""
        print(f"\n📖 Reading records from {table_name}:")
        try:
            query = f"SELECT * FROM {table_name} LIMIT 10"
            rows = self.session.execute(query)
            
            if not rows:
                print("No records found.")
                return
            
            print("-" * 80)
            for row in rows:
                print(row)
            print("-" * 80)
            print(f"Displayed first 10 records from {table_name}")
        except Exception as e:
            print(f"Error reading records: {e}")
    
    def delete_record(self, table_name):
        """Delete record from a table"""
        print(f"\nDelete record from {table_name}:")
        print("Note: You need to provide primary key values")
        
        if table_name == "kho_sp_theo_ma_cn":
            try:
                ma_chi_nhanh = int(input("Mã chi nhánh: "))
                ma_san_pham = input("Mã sản phẩm: ")
                tong_so_luong_ton_kho = int(input("Tổng số lượng tồn kho: "))
                
                query = """
                DELETE FROM kho_sp_theo_ma_cn 
                WHERE ma_chi_nhanh = ? AND ma_san_pham = ? AND tong_so_luong_ton_kho = ?
                """
                self.session.execute(query, (ma_chi_nhanh, ma_san_pham, tong_so_luong_ton_kho))
                print("✅ Record deleted successfully!")
            except Exception as e:
                print(f"❌ Error deleting record: {e}")
        else:
            print("❌ Delete operation not implemented for this table yet.")
    
    def run(self):
        """Main program loop"""
        tables = {
            1: "chi_tiet_hoa_don_theo_ma_kh",
            2: "doanh_thu_moi_ngay_theo_ma_cn",
            3: "kho_sp_theo_ma_cn",
            4: "sl_khach_hang_moi_ngay_theo_ma_cn",
            5: "doanh_thu_sp_quy_cn",
            6: "doanh_thu_thang_nv_cn"
        }
        
        while True:
            self.display_main_menu()
            
            try:
                choice = int(input("\nChoose (0-6): "))
                
                if choice == 0:
                    print("👋 Goodbye!")
                    break
                elif choice in tables:
                    table_name = tables[choice]
                    
                    while True:
                        self.display_crud_menu(table_name)
                        crud_choice = int(input("\n🎯 Choose operation (0-4): "))
                        
                        if crud_choice == 0:
                            break
                        elif crud_choice == 1:  # CREATE
                            if choice == 1:
                                self.insert_chi_tiet_hoa_don()
                            elif choice == 3:
                                self.insert_kho_sp()
                            else:
                                print("❌ Insert operation not implemented for this table yet.")
                        elif crud_choice == 2:  # READ
                            self.read_records(table_name)
                        elif crud_choice == 3:  # UPDATE
                            print("❌ Update operation not implemented yet.")
                        elif crud_choice == 4:  # DELETE
                            self.delete_record(table_name)
                        else:
                            print("❌ Invalid choice! Please choose 0-4.")
                else:
                    print("❌ Invalid choice! Please choose 0-6.")
            except ValueError:
                print("❌ Invalid input! Please enter a number.")
            except KeyboardInterrupt:
                print("\n👋 Program interrupted. Goodbye!")
                break
        
        self.disconnect()

if __name__ == "__main__":
    crud_app = CassandraCRUD()
    crud_app.run()