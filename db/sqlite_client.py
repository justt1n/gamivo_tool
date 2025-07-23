import logging
import sqlite3
from sqlite3 import Error
from typing import List, Any, Optional


class SQLiteClient:
    """A wrapper for SQLite database connections providing basic functionalities."""

    def __init__(self, db_file: str):
        """
        Initializes the SQLiteDB object.

        Args:
            db_file (str): The path to the SQLite database file.
        """
        self.db_file = db_file
        self.conn: Optional[sqlite3.Connection] = None

    def __enter__(self):
        """Enter the runtime context related to this object."""
        self.create_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        self.close_connection()

    def create_connection(self):
        """Creates a database connection to the SQLite database."""
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
            List[Any]: A list of rows fetched from the database.
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
