import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Optional, Tuple, Union
import logging

from SysConfig import SysConfig

class TableNames:
    ACTION = "xcam_actions"
    CAMERA = "xcam_cameras"

class ColNames:
    ID = 'id'
    COMMAND = 'command'
    MAC_ADDRESS = 'mac_address'
    IP_ADDRESS = 'ip_address'
    IP_TYPE = 'ip_type'
    UPDATED_AT = 'updated_at'
    CREATED_AT = 'created_at'
    STATUS = 'status'
    ADDITIONS = 'additions'

class ActionStatus:
    PENDING = 'pending'
    IN_PROGRESS = 'in_progress'
    DONE = 'done'
    FAILED = 'failed'

class DbHelper:
    """
    Common MySQL Database Helper Class
    Provides easy-to-use methods for SELECT, INSERT, UPDATE, DELETE operations
    Including batch operations for better performance
    """

    def __init__(self, host="192.168.1.22", user="root", password="root", database="testuse_config"):
        """
        Initialize database connection parameters
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def update_by_sys_config(self, sys_config: SysConfig):
        self.host = sys_config.db_host
        self.user = sys_config.db_user
        self.password = sys_config.db_password
        self.database = sys_config.db_name

    def get_connection(self) -> mysql.connector.MySQLConnection:
        """
        Create and return database connection with better connection handling
        """
        try:
            # Always check if connection is still valid
            if self.connection is None or not self.connection.is_connected():
                if self.connection:
                    try:
                        self.connection.close()
                    except:
                        pass
                        
                self.connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    autocommit=True,  # Enable autocommit for SELECT queries
                    connection_timeout=10,
                    pool_reset_session=True
                )
                self.logger.info("Database connection established")
            else:
                # Test connection with ping
                try:
                    self.connection.ping(reconnect=True, attempts=3, delay=1)
                except:
                    # If ping fails, create new connection
                    self.connection = mysql.connector.connect(
                        host=self.host,
                        user=self.user,
                        password=self.password,
                        database=self.database,
                        autocommit=True,
                        connection_timeout=10,
                        pool_reset_session=True
                    )
                    self.logger.info("Database connection reconnected")
                    
            return self.connection
        except Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise

    def close_connection(self):
        """
        Close database connection and cursor
        """
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection and self.connection.is_connected():
                self.connection.close()
                self.connection = None
                self.logger.info("Database connection closed")
        except Error as e:
            self.logger.error(f"Error closing connection: {e}")

    def __enter__(self):
        """Context manager entry"""
        self.get_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_connection()

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Execute SELECT query and return results with better error handling
        """
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            results = cursor.fetchall()
            self.logger.debug(f"Query executed successfully. Returned {len(results)} rows")
            return results

        except Error as e:
            self.logger.error(f"Error executing query: {e}")
            self.logger.error(f"Query: {query}")
            if params:
                self.logger.error(f"Params: {params}")
            raise
        finally:
            if cursor:
                cursor.close()

    def execute_query_dict(self, query: str, params: Optional[Tuple] = None) -> List[Dict]:
        """
        Execute SELECT query and return results as list of dictionaries with better connection handling
        """
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            results = cursor.fetchall()
            self.logger.debug(f"Query executed successfully. Returned {len(results)} rows")
            self.logger.debug(f"Query: {query}")
            if params:
                self.logger.debug(f"Params: {params}")
            return results

        except Error as e:
            self.logger.error(f"Error executing query: {e}")
            self.logger.error(f"Query: {query}")
            if params:
                self.logger.error(f"Params: {params}")
            raise
        finally:
            if cursor:
                cursor.close()

    def select_all(self, table: str,
                   conditions: Optional[str] = None,
                   params: Optional[Tuple] = None,
                   offset=None,
                   limit=None
                   ) -> List[Dict]:
        """
        Select all records from table with optional conditions
        """
        query = f"SELECT * FROM {table}"
        if conditions:
            query += f" WHERE {conditions}"

        if limit and offset:
            query += f" LIMIT {offset}, {limit}"
        elif limit:
            query += f" LIMIT {limit}"

        return self.execute_query_dict(query, params)

    def select_by_id(self, table: str, id_value: Any, id_column: str = "id") -> Optional[Dict]:
        """
        Select single record by ID
        """
        query = f"SELECT * FROM {table} WHERE {id_column} = %s"
        results = self.execute_query_dict(query, (id_value,))
        return results[0] if results else None

    def insert_one(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert single record into table with transaction control
        """
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

            cursor.execute(query, tuple(data.values()))
            if not conn.autocommit:
                conn.commit()

            last_id = cursor.lastrowid
            self.logger.info(f"Record inserted successfully. ID: {last_id}")
            return last_id

        except Error as e:
            if not conn.autocommit:
                conn.rollback()
            self.logger.error(f"Error inserting record: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def insert_batch(self, table: str, data_list: List[Dict[str, Any]]) -> int:
        """
        Insert multiple records in batch
        """
        if not data_list:
            return 0

        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get columns from first record
            columns = ", ".join(data_list[0].keys())
            placeholders = ", ".join(["%s"] * len(data_list[0]))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

            # Prepare data for executemany
            values_list = [tuple(record.values()) for record in data_list]

            cursor.executemany(query, values_list)
            if not conn.autocommit:
                conn.commit()

            affected_rows = cursor.rowcount
            self.logger.info(f"Batch insert completed. {affected_rows} records inserted")
            return affected_rows

        except Error as e:
            if not conn.autocommit:
                conn.rollback()
            self.logger.error(f"Error in batch insert: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def update_by_id(self, table: str, id_value: Any, data: Dict[str, Any], id_column: str = "id") -> int:
        """
        Update record by ID with transaction control
        """
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
            query = f"UPDATE {table} SET {set_clause} WHERE {id_column} = %s"

            params = tuple(data.values()) + (id_value,)
            cursor.execute(query, params)
            if not conn.autocommit:
                conn.commit()

            affected_rows = cursor.rowcount
            self.logger.info(f"Update completed. {affected_rows} rows affected")
            return affected_rows

        except Error as e:
            if not conn.autocommit:
                conn.rollback()
            self.logger.error(f"Error updating record: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def update_with_conditions(self, table: str, data: Dict[str, Any], conditions: str,
                               params: Optional[Tuple] = None) -> int:
        """
        Update records with custom conditions
        """
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
            query = f"UPDATE {table} SET {set_clause} WHERE {conditions}"

            update_params = tuple(data.values())
            if params:
                update_params += params

            cursor.execute(query, update_params)
            if not conn.autocommit:
                conn.commit()

            affected_rows = cursor.rowcount
            self.logger.info(f"Update completed. {affected_rows} rows affected")
            return affected_rows

        except Error as e:
            if not conn.autocommit:
                conn.rollback()
            self.logger.error(f"Error updating records: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def update_batch(self, table: str, data_list: List[Dict[str, Any]], id_column: str = "id") -> int:
        """
        Update multiple records in batch
        """
        if not data_list:
            return 0

        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            total_affected = 0

            for data in data_list:
                if id_column not in data:
                    raise ValueError(f"ID column '{id_column}' not found in data")

                id_value = data.pop(id_column)

                set_clause = ", ".join([f"{key} = %s" for key in data.keys()])
                query = f"UPDATE {table} SET {set_clause} WHERE {id_column} = %s"

                params = tuple(data.values()) + (id_value,)
                cursor.execute(query, params)
                total_affected += cursor.rowcount

                data[id_column] = id_value

            if not conn.autocommit:
                conn.commit()
            self.logger.info(f"Batch update completed. {total_affected} rows affected")
            return total_affected

        except Error as e:
            if not conn.autocommit:
                conn.rollback()
            self.logger.error(f"Error in batch update: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def delete_by_id(self, table: str, id_value: Any, id_column: str = "id") -> int:
        """
        Delete record by ID
        """
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            query = f"DELETE FROM {table} WHERE {id_column} = %s"
            cursor.execute(query, (id_value,))
            if not conn.autocommit:
                conn.commit()

            affected_rows = cursor.rowcount
            self.logger.info(f"Delete completed. {affected_rows} rows affected")
            return affected_rows

        except Error as e:
            if not conn.autocommit:
                conn.rollback()
            self.logger.error(f"Error deleting record: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def delete_with_conditions(self, table: str, conditions: str, params: Optional[Tuple] = None) -> int:
        """
        Delete records with custom conditions
        """
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            query = f"DELETE FROM {table} WHERE {conditions}"

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if not conn.autocommit:
                conn.commit()

            affected_rows = cursor.rowcount
            self.logger.info(f"Delete completed. {affected_rows} rows affected")
            return affected_rows

        except Error as e:
            if not conn.autocommit:
                conn.rollback()
            self.logger.error(f"Error deleting records: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def count_records(self, table: str, conditions: Optional[str] = None, params: Optional[Tuple] = None) -> int:
        """
        Count records in table with optional conditions
        """
        query = f"SELECT COUNT(*) FROM {table}"
        if conditions:
            query += f" WHERE {conditions}"

        results = self.execute_query(query, params)
        return results[0][0] if results else 0

    def insert_or_update_batch(self, table: str, data_list: List[Dict[str, Any]],
                               unique_columns: Union[str, List[str]],
                               update_columns: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Insert batch records, if unique columns exist then update specified columns
        """
        if not data_list:
            return {"inserted": 0, "updated": 0}

        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Normalize unique_columns to list
            if isinstance(unique_columns, str):
                unique_columns = [unique_columns]

            # Get all columns from first record
            all_columns = list(data_list[0].keys())

            # Determine columns to update
            if update_columns is None:
                update_columns = [col for col in all_columns if col not in unique_columns]

            # Validate that unique columns exist in data
            for col in unique_columns:
                if col not in all_columns:
                    raise ValueError(f"Unique column '{col}' not found in data")

            # Validate that update columns exist in data
            for col in update_columns:
                if col not in all_columns:
                    raise ValueError(f"Update column '{col}' not found in data")

            # Build INSERT part
            columns = ", ".join(all_columns)
            placeholders = ", ".join(["%s"] * len(all_columns))

            # Build ON DUPLICATE KEY UPDATE part
            update_clause = ", ".join([f"{col} = VALUES({col})" for col in update_columns])

            query = f"""
            INSERT INTO {table} ({columns}) 
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
            """

            # Prepare data for executemany
            values_list = [tuple(record.values()) for record in data_list]

            # Execute batch insert/update
            cursor.executemany(query, values_list)
            if not conn.autocommit:
                conn.commit()

            # Calculate inserted vs updated records
            total_affected = cursor.rowcount

            estimated_updated = max(0, total_affected - len(data_list))
            estimated_inserted = len(data_list) - estimated_updated

            result = {
                "inserted": estimated_inserted,
                "updated": estimated_updated,
                "total_affected": total_affected
            }

            self.logger.info(
                f"Batch upsert completed. Estimated - Inserted: {estimated_inserted}, Updated: {estimated_updated}")
            return result

        except Error as e:
            if not conn.autocommit:
                conn.rollback()
            self.logger.error(f"Error in batch upsert: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def insert_or_update_batch_precise(self, table: str, data_list: List[Dict[str, Any]],
                                       unique_columns: Union[str, List[str]],
                                       update_columns: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Precise insert or update batch with better transaction handling
        """
        if not data_list:
            return {"inserted": 0, "updated": 0}

        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Temporarily disable autocommit for transaction
            original_autocommit = conn.autocommit
            conn.autocommit = False

            # Normalize unique_columns to list
            if isinstance(unique_columns, str):
                unique_columns = [unique_columns]

            # Get all columns from first record
            all_columns = list(data_list[0].keys())

            # Determine columns to update
            if update_columns is None:
                update_columns = [col for col in all_columns if col not in unique_columns]

            inserted_count = 0
            updated_count = 0

            for data in data_list:
                # Build condition to check if record exists
                conditions = " AND ".join([f"{col} = %s" for col in unique_columns])
                condition_values = tuple(data[col] for col in unique_columns)

                # Check if record exists
                check_query = f"SELECT 1 FROM {table} WHERE {conditions} LIMIT 1"
                cursor.execute(check_query, condition_values)
                exists = cursor.fetchone() is not None

                if exists:
                    # Update existing record
                    if update_columns:
                        set_clause = ", ".join([f"{col} = %s" for col in update_columns])
                        update_query = f"UPDATE {table} SET {set_clause} WHERE {conditions}"

                        update_values = tuple(data[col] for col in update_columns) + condition_values
                        cursor.execute(update_query, update_values)

                        if cursor.rowcount > 0:
                            updated_count += 1
                else:
                    # Insert new record
                    columns = ", ".join(all_columns)
                    placeholders = ", ".join(["%s"] * len(all_columns))
                    insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

                    cursor.execute(insert_query, tuple(data.values()))
                    inserted_count += 1

            conn.commit()
            conn.autocommit = original_autocommit

            result = {
                "inserted": inserted_count,
                "updated": updated_count,
                "total_affected": inserted_count + updated_count
            }

            self.logger.info(f"Precise batch upsert completed. Inserted: {inserted_count}, Updated: {updated_count}")
            return result

        except Error as e:
            conn.rollback()
            conn.autocommit = original_autocommit
            self.logger.error(f"Error in precise batch upsert: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def select_first_order_by(self, table: str, col_name: str, sort_type: str = 'ASC',
                              conditions: Optional[str] = None, params: Optional[Tuple] = None) -> Optional[Dict]:
        """
        Select first record ordered by specified column with fresh connection
        """
        # Force fresh connection for SELECT queries to avoid stale data
        if self.connection:
            try:
                self.connection.ping(reconnect=True, attempts=1, delay=0)
            except:
                # If ping fails, close connection to force new one
                self.close_connection()
        
        # Validate sort_type to prevent SQL injection
        sort_type = sort_type.upper()
        if sort_type not in ['ASC', 'DESC']:
            raise ValueError("sort_type must be 'ASC' or 'DESC'")

        # Build query
        query = f"SELECT * FROM {table}"

        if conditions:
            query += f" WHERE {conditions}"

        query += f" ORDER BY {col_name} {sort_type} LIMIT 1"

        results = self.execute_query_dict(query, params)
        return results[0] if results else None

    def select_first_multiple_order_by(self, table: str, order_columns: List[Tuple[str, str]],
                                       conditions: Optional[str] = None, params: Optional[Tuple] = None) -> Optional[Dict]:
        """
        Select first record ordered by multiple columns
        """
        if not order_columns:
            raise ValueError("order_columns cannot be empty")

        # Validate and build ORDER BY clause
        order_parts = []
        for col_name, sort_type in order_columns:
            sort_type = sort_type.upper()
            if sort_type not in ['ASC', 'DESC']:
                raise ValueError(f"sort_type must be 'ASC' or 'DESC', got '{sort_type}' for column '{col_name}'")
            order_parts.append(f"{col_name} {sort_type}")

        order_clause = ", ".join(order_parts)

        # Build query
        query = f"SELECT * FROM {table}"

        if conditions:
            query += f" WHERE {conditions}"

        query += f" ORDER BY {order_clause} LIMIT 1"

        results = self.execute_query_dict(query, params)
        return results[0] if results else None

    def select_top_n_order_by(self, table: str, col_name: str, sort_type: str = 'ASC',
                              limit: int = 10, conditions: Optional[str] = None,
                              params: Optional[Tuple] = None) -> List[Dict]:
        """
        Select top N records ordered by specified column
        """
        # Validate parameters
        sort_type = sort_type.upper()
        if sort_type not in ['ASC', 'DESC']:
            raise ValueError("sort_type must be 'ASC' or 'DESC'")

        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        # Build query
        query = f"SELECT * FROM {table}"

        if conditions:
            query += f" WHERE {conditions}"

        query += f" ORDER BY {col_name} {sort_type} LIMIT {limit}"

        return self.execute_query_dict(query, params)

    # Convenience methods for common use cases
    def select_oldest(self, table: str, date_column: str = 'created_at',
                      conditions: Optional[str] = None, params: Optional[Tuple] = None) -> Optional[Dict]:
        """Get oldest record by date column"""
        return self.select_first_order_by(table, date_column, 'ASC', conditions, params)

    def select_newest(self, table: str, date_column: str = 'created_at',
                      conditions: Optional[str] = None, params: Optional[Tuple] = None) -> Optional[Dict]:
        """Get newest record by date column"""
        return self.select_first_order_by(table, date_column, 'DESC', conditions, params)

    def select_min_value(self, table: str, col_name: str,
                         conditions: Optional[str] = None, params: Optional[Tuple] = None) -> Optional[Dict]:
        """Get record with minimum value in specified column"""
        return self.select_first_order_by(table, col_name, 'ASC', conditions, params)

    def select_max_value(self, table: str, col_name: str,
                         conditions: Optional[str] = None, params: Optional[Tuple] = None) -> Optional[Dict]:
        """Get record with maximum value in specified column"""
        return self.select_first_order_by(table, col_name, 'DESC', conditions, params)
