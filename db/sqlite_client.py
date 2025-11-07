import logging
import sqlite3
from sqlite3 import Error
from typing import List, Any, Optional, Dict



class SQLiteClient:
    """
    A wrapper for SQLite database connections providing basic functionalities,
    with an added internal cache for product-offer mapping.
    """

    def __init__(self, db_file: str):
        """
        Initializes the SQLiteDB object and automatically loads the product-offer map into cache.

        Args:
            db_file (str): The path to the SQLite database file.
        """
        self.db_file = db_file
        self.conn: Optional[sqlite3.Connection] = None

        # Khởi tạo Map (Dictionary) rỗng trước
        self.product_offer_map: Dict[int, int] = {}

        # Tự động Tải dữ liệu vào Map ngay khi khởi tạo
        # (Chỉ nên làm khi dữ liệu không thay đổi và có kích thước nhỏ)
        self.load_product_offer_map()

    def __enter__(self):
        """Enter the runtime context related to this object."""
        self.create_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        self.close_connection()

    def create_connection(self):
        """
        Creates a database connection to the SQLite database.
        """
        if self.conn:
            return
        try:
            self.conn = sqlite3.connect(self.db_file)
            logging.info(f"SQLite version {sqlite3.version} connected successfully to {self.db_file}")
        except Error as e:
            logging.error(f"Error connecting to database {self.db_file}: {e}")
            raise

    def close_connection(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logging.info(f"SQLite connection to {self.db_file} closed.")

    def fetch_query(self, query: str, params: tuple = ()) -> List[Any]:
        """
        Executes a SELECT query and returns the fetched results.

        Args:
            query (str): The SQL query to execute.
            params (tuple): Optional parameters to bind to the query.

        Returns:
            List[Any]: A list of rows fetched from the database (as tuples).
        """
        if not self.conn:
            self.create_connection()

        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()
        except Error as e:
            logging.error(f"Failed to fetch query '{query}': {e}")
            return []

    # --- Caching Methods ---

    def load_product_offer_map(self):
        """
        Loads the mapping of product_id (key) to offer_id (value)
        from 'product_offers' table into the internal memory map.
        """
        query = "SELECT product_id, id FROM product_offers"

        try:
            # SỬ DỤNG CONTEXT MANAGER để kết nối và đóng kết nối an toàn
            with self:
                results = self.fetch_query(query)

            new_map = {}
            for row in results:
                # Giả định hàng trả về là tuple: (product_id, offer_id)
                product_id, offer_id = row
                new_map[int(product_id)] = int(offer_id)

            # Cập nhật cache
            self.product_offer_map = new_map
            logging.info(f"Successfully loaded {len(self.product_offer_map)} product-offer mappings into cache.")

        except Error as e:
            logging.error(f"Failed to load product-offer map from database: {e}")
        except Exception as e:
            # Ghi log lỗi để dễ dàng debug nếu có vấn đề về kết nối hoặc cấu trúc bảng
            logging.error(f"An unexpected error occurred during map loading: {e}")

    def get_offer_id_by_product_id(self, product_id: int) -> Optional[int]:
        """
        Retrieves the offer ID using a product ID by looking up the in-memory cache map.

        Args:
            product_id (int): The ID of the product.

        Returns:
            Optional[int]: The corresponding offer ID, or None if not found in cache.
        """
        # TRUY CẬP CACHE: Thao tác an toàn và cực nhanh trong môi trường đa luồng.
        offer_id = self.product_offer_map.get(product_id)

        if offer_id is not None:
            return offer_id

        logging.warning(f"No offer ID found in cache for product_id: {product_id}")
        return None