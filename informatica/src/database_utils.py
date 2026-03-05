"""
Database utility functions for Full Refresh workflow
Handles connections, truncation, and constraint management
"""

import psycopg2
from psycopg2.extras import execute_batch
from contextlib import contextmanager
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, config: Dict[str, Any]):
        self.staging_config = config['database']['staging']
        self.warehouse_config = config['database']['warehouse']
        
    @contextmanager
    def get_staging_connection(self):
        """Get staging database connection"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.staging_config['host'],
                port=self.staging_config['port'],
                database=self.staging_config['database'],
                user=self.staging_config['username'],
                password=self.staging_config['password']
            )
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Staging database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_warehouse_connection(self):
        """Get warehouse database connection"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.warehouse_config['host'],
                port=self.warehouse_config['port'],
                database=self.warehouse_config['database'],
                user=self.warehouse_config['username'],
                password=self.warehouse_config['password']
            )
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Warehouse database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def truncate_table(self, table_name: str, warehouse: bool = True) -> int:
        """
        Truncate a table
        
        Args:
            table_name: Name of table to truncate
            warehouse: If True, use warehouse DB; else use staging DB
            
        Returns:
            Number of rows deleted
        """
        connection_manager = (
            self.get_warehouse_connection if warehouse 
            else self.get_staging_connection
        )
        
        with connection_manager() as conn:
            cursor = conn.cursor()
            
            # Get row count before truncate
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Truncate table
            cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
            
            logger.info(f"Truncated {table_name}: {row_count} rows removed")
            return row_count
    
    def disable_constraints(self, table_names: List[str]) -> None:
        """Disable foreign key constraints for tables"""
        with self.get_warehouse_connection() as conn:
            cursor = conn.cursor()
            
            for table_name in table_names:
                cursor.execute(f"""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_name = %s 
                    AND constraint_type = 'FOREIGN KEY'
                """, (table_name.lower(),))
                
                constraints = cursor.fetchall()
                
                for (constraint_name,) in constraints:
                    cursor.execute(f"""
                        ALTER TABLE {table_name} 
                        DISABLE TRIGGER {constraint_name}
                    """)
                    logger.info(f"Disabled constraint {constraint_name} on {table_name}")
    
    def enable_constraints(self, table_names: List[str]) -> None:
        """Enable foreign key constraints for tables"""
        with self.get_warehouse_connection() as conn:
            cursor = conn.cursor()
            
            for table_name in table_names:
                cursor.execute(f"""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_name = %s 
                    AND constraint_type = 'FOREIGN KEY'
                """, (table_name.lower(),))
                
                constraints = cursor.fetchall()
                
                for (constraint_name,) in constraints:
                    cursor.execute(f"""
                        ALTER TABLE {table_name} 
                        ENABLE TRIGGER {constraint_name}
                    """)
                    logger.info(f"Enabled constraint {constraint_name} on {table_name}")
    
    def rebuild_indexes(self, table_name: str) -> None:
        """Rebuild indexes for a table"""
        with self.get_warehouse_connection() as conn:
            cursor = conn.cursor()
            
            # Get all indexes for the table
            cursor.execute(f"""
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename = %s
            """, (table_name.lower(),))
            
            indexes = cursor.fetchall()
            
            for (index_name,) in indexes:
                cursor.execute(f"REINDEX INDEX {index_name}")
                logger.info(f"Rebuilt index {index_name}")
    
    def update_statistics(self, table_name: str) -> None:
        """Update table statistics"""
        with self.get_warehouse_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"ANALYZE {table_name}")
            logger.info(f"Updated statistics for {table_name}")
    
    def get_row_count(self, table_name: str, warehouse: bool = True) -> int:
        """Get row count for a table"""
        connection_manager = (
            self.get_warehouse_connection if warehouse 
            else self.get_staging_connection
        )
        
        with connection_manager() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            return count
    
    def execute_query(self, query: str, params: Optional[tuple] = None, 
                     warehouse: bool = True) -> List[tuple]:
        """Execute a query and return results"""
        connection_manager = (
            self.get_warehouse_connection if warehouse 
            else self.get_staging_connection
        )
        
        with connection_manager() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()