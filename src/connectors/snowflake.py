"""
Snowflake connector implementation for the GDPR tagger with environment variable support.
"""

import os
import re
import logging
from typing import Dict, List, Optional, Any

from dotenv import load_dotenv

from .base import DatabaseConnector

logger = logging.getLogger(__name__)

class SnowflakeConnector(DatabaseConnector):
    """Connector implementation for Snowflake with environment variable support"""
    
    def __init__(self, config: Dict[str, str]):
        """Initialize with connection parameters"""
        # Load environment variables
        load_dotenv()
        
        # Process the configuration to replace environment variables
        self.config = self._process_env_variables(config)
        self.conn = None
        
        # Import here to make the dependency optional
        try:
            import snowflake.connector
            self.snowflake_connector = snowflake.connector
        except ImportError:
            logger.error("Snowflake connector not installed. Please install with: pip install snowflake-connector-python")
            raise ImportError("Required package 'snowflake-connector-python' not installed. Please install it with pip.")
    
    def _process_env_variables(self, config: Dict[str, str]) -> Dict[str, str]:
        """Replace environment variable placeholders in config values"""
        processed_config = {}
        
        for key, value in config.items():
            if isinstance(value, str):
                # Look for ${ENV_VAR} pattern
                matches = re.findall(r'\${([A-Za-z0-9_]+)}', value)
                if matches:
                    for env_var in matches:
                        env_value = os.getenv(env_var)
                        if env_value:
                            value = value.replace(f'${{{env_var}}}', env_value)
                        else:
                            logger.warning(f"Environment variable {env_var} not found. Using empty string.")
                            value = value.replace(f'${{{env_var}}}', '')
            processed_config[key] = value
            
        return processed_config
    
    def connect(self) -> Any:
        """Connect to Snowflake database"""
        self.conn = self.snowflake_connector.connect(
            user=self.config.get('user'),
            password=self.config.get('password'),
            account=self.config.get('account'),
            warehouse=self.config.get('warehouse'),
            database=self.config.get('database'),
            role=self.config.get('role', 'ACCOUNTADMIN')
        )
        logger.info(f"Connected to Snowflake database: {self.config.get('database')}")
        return self.conn
    
    def get_schemas(self) -> List[str]:
        """Get list of schemas in the database"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("SHOW SCHEMAS")
            schemas = [row[1] for row in cursor.fetchall()]
            return schemas
        finally:
            cursor.close()
    
    def get_tables(self, schema: str) -> List[str]:
        """Get list of tables in a schema"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SHOW TABLES IN SCHEMA {schema}")
            tables = [row[1] for row in cursor.fetchall()]
            return tables
        finally:
            cursor.close()
    
    def get_columns(self, schema: str, table: str) -> List[Dict[str, str]]:
        """Get column information for a table"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"DESCRIBE TABLE {schema}.{table}")
            columns = []
            for row in cursor.fetchall():
                column_info = {
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[3]
                }
                columns.append(column_info)
            return columns
        finally:
            cursor.close()
    
    def get_sample_data(self, schema: str, table: str, column: str, sample_size: int = 100) -> List[Any]:
        """Get sample data from a column"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT {column} FROM {schema}.{table} SAMPLE ({sample_size} ROWS)")
            # Extract values from the single-column result
            sample_data = [row[0] for row in cursor.fetchall() if row[0] is not None]
            return sample_data
        finally:
            cursor.close()
    
    def apply_tag(self, schema: str, table: str, column: str, tag: str, tag_value: str, tag_schema: str = "") -> bool:
        """Apply a tag to a column using Snowflake's tag mechanism"""
        cursor = self.conn.cursor()
        try:
            # Define the tag schema - use provided tag_schema if not empty, otherwise use the table's schema
            if tag_schema:
                used_tag_schema = tag_schema
            else:
                used_tag_schema = schema  # Default: use the same schema as the tagged object
            
            # First ensure the schema exists for tags
            cursor.execute(f"USE DATABASE {self.config.get('database')}")
            cursor.execute(f"USE SCHEMA {used_tag_schema}")
            
            # Check if tag exists, create it if not
            cursor.execute(f"SHOW TAGS LIKE '{tag}'")
            if not cursor.fetchone():
                cursor.execute(f"CREATE TAG IF NOT EXISTS {tag}")
                logger.info(f"Created new tag: {tag} in schema {used_tag_schema}")
            
            # Apply tag to column - use fully qualified names
            cursor.execute(f"ALTER TABLE {schema}.{table} MODIFY COLUMN {column} SET TAG {used_tag_schema}.{tag} = '{tag_value}'")
            logger.info(f"Applied tag {used_tag_schema}.{tag}='{tag_value}' to {schema}.{table}.{column}")
            return True
        except Exception as e:
            logger.error(f"Failed to apply tag: {e}")
            return False
        finally:
            cursor.close()
    
    def close(self) -> None:
        """Close the Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed Snowflake connection")
            self.conn = None