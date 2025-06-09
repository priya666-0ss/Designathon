import psycopg2
from psycopg2 import Error
from typing import List, Dict, Any, Tuple, Optional
from contextlib import contextmanager

class DatabaseConnection:
    def __init__(self):
        self.connection_params = {
            'dbname': 'db_pls',
            'user': 'db_pls_user',
            'password': 'password123',
            'host': '127.0.0.1',
            'port': '5432'
        }

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
        except Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def execute_query(self, query: str, params: Tuple = None) -> Optional[List[Dict]]:
        """Execute a query and return results if any"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(query, params)
                    if cursor.description:  # If the query returns data
                        columns = [desc[0] for desc in cursor.description]
                        results = cursor.fetchall()
                        return [dict(zip(columns, row)) for row in results]
                    conn.commit()
                    return None
                except Error as e:
                    conn.rollback()
                    print(f"Query execution error: {e}")
                    raise

    # CREATE operations
    def create_table(self, table_name: str, columns: Dict[str, str]) -> None:
        """Create a new table"""
        columns_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"
        self.execute_query(query)

    def insert_record(self, table_name: str, data: Dict[str, Any]) -> None:
        """Insert a single record into a table"""
        columns = ", ".join(data.keys())
        values = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        self.execute_query(query, tuple(data.values()))

    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Insert multiple records into a table"""
        if not data:
            return
        columns = ", ".join(data[0].keys())
        values = ", ".join(["%s"] * len(data[0]))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.executemany(query, [tuple(record.values()) for record in data])
                    conn.commit()
                except Error as e:
                    conn.rollback()
                    print(f"Bulk insert error: {e}")
                    raise

    # READ operations
    def select_all(self, table_name: str) -> List[Dict]:
        """Select all records from a table"""
        query = f"SELECT * FROM {table_name}"
        return self.execute_query(query)

    def select_by_condition(self, table_name: str, conditions: Dict[str, Any]) -> List[Dict]:
        """Select records based on conditions"""
        where_clause = " AND ".join([f"{k} = %s" for k in conditions.keys()])
        query = f"SELECT * FROM {table_name} WHERE {where_clause}"
        return self.execute_query(query, tuple(conditions.values()))

    def select_custom(self, query: str, params: Tuple = None) -> List[Dict]:
        """Execute a custom SELECT query"""
        return self.execute_query(query, params)

    # UPDATE operations
    def update_records(self, table_name: str, updates: Dict[str, Any], 
                      conditions: Dict[str, Any]) -> None:
        """Update records that match the conditions"""
        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
        where_clause = " AND ".join([f"{k} = %s" for k in conditions.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        params = tuple(list(updates.values()) + list(conditions.values()))
        self.execute_query(query, params)

    # DELETE operations
    def delete_records(self, table_name: str, conditions: Dict[str, Any]) -> None:
        """Delete records that match the conditions"""
        where_clause = " AND ".join([f"{k} = %s" for k in conditions.keys()])
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        self.execute_query(query, tuple(conditions.values()))

    def truncate_table(self, table_name: str) -> None:
        """Delete all records from a table"""
        query = f"TRUNCATE TABLE {table_name}"
        self.execute_query(query)

    # Utility functions
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """
        result = self.execute_query(query, (table_name,))
        return result[0]['exists']

    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get the schema of a table"""
        query = """
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s;
        """
        return self.execute_query(query, (table_name,))
