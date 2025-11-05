import sqlite3
import logging
from typing import Optional, List, Tuple, Any


class SQLiteClient:

    def __init__(self, db_path: str):
        if not db_path:
            raise ValueError("Đường dẫn DB không được để trống.")
        self.db_path = db_path
        logging.info(f"SQLiteClient khởi tạo cho CSDL tại: {self.db_path}")

    def fetch_query(self, query: str, params: Tuple = ()) -> List[Tuple]:
        """
        Thực thi một truy vấn SELECT một cách an toàn (thread-safe).
        Mở, truy vấn, và đóng kết nối trong cùng một hàm.
        """
        conn: Optional[sqlite3.Connection] = None
        try:
            # 1. Tạo kết nối (chỉ tồn tại trong hàm này)
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()

            # 2. Thực thi
            cur.execute(query, params)

            # 3. Lấy kết quả
            results = cur.fetchall()
            return results
        except sqlite3.Error as e:
            # Lỗi này sẽ được bắt bởi asyncio.to_thread và ném ra ngoài
            logging.error(f"Lỗi SQLite khi thực thi '{query}': {e}")
            # Trả về list rỗng hoặc ném lỗi tùy logic của bạn
            # Ở đây chúng ta ném lỗi để hàm gọi (get_offer_id_by_product_id) biết
            raise
        finally:
            # 4. Đóng kết nối (luôn luôn)
            if conn:
                conn.close()

    # Các hàm create_connection và close_connection không còn cần thiết
    # vì mỗi hàm fetch sẽ tự quản lý kết nối của nó.