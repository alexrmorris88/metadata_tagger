"""
Policy engine module for executing Snowflake security policies.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)

class PolicyEngine:
    """Executes Snowflake security policies"""
    
    def __init__(self, connector):
        """Initialize with a database connector"""
        self.connector = connector
        self._active_schema = None
    
    def get_tagged_columns(self, database: str, tag_name: str, categories: List[str]) -> List[Dict[str, str]]:
        """
        Get all columns with the specified tag and categories
        Using SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES view with improved detection
        """
        try:
            cursor = self.connector.conn.cursor()
            
            # Convert tag_name to uppercase for case-insensitive comparison
            tag_name_upper = tag_name.upper()
            
            # Log all categories we're looking for
            logger.info(f"Searching for columns with tag '{tag_name}' and categories: {categories}")
            
            # Create a more flexible category filter that does partial matching
            category_conditions = []
            for cat in categories:
                # Use ILIKE for case-insensitive matching and % for partial matching
                category_conditions.append(f"UPPER(TAG_VALUE) ILIKE '%{cat.upper()}%'")
            
            category_filter = " OR ".join(category_conditions)
            
            # First, query to see all available tags for debugging
            debug_sql = f"""
            SELECT DISTINCT TAG_NAME, TAG_VALUE 
            FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
            WHERE OBJECT_DATABASE = '{database}'
            """
            
            cursor.execute(debug_sql)
            all_tags = cursor.fetchall()
            logger.info(f"All available tags in database {database}:")
            for tag in all_tags:
                logger.info(f"  - {tag[0]}: {tag[1]}")
            
            # Now query for tagged columns using ACCOUNT_USAGE with improved matching
            sql = f"""
            SELECT DISTINCT
                OBJECT_DATABASE,
                OBJECT_SCHEMA,
                OBJECT_NAME,
                COLUMN_NAME,
                TAG_NAME,
                TAG_VALUE
            FROM 
                SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
            WHERE 
                OBJECT_DATABASE = '{database}'
                AND UPPER(TAG_NAME) = '{tag_name_upper}'
                AND ({category_filter})
            """
            
            logger.debug(f"Executing query: {sql}")
            cursor.execute(sql)
            results = cursor.fetchall()
            
            # Log raw results for debugging
            logger.info(f"Raw results from tag query: {len(results)} rows")
            for row in results:
                logger.info(f"Tag result: {row}")
            
            # Group by table to efficiently get data types
            tables = {}
            for row in results:
                schema = row[1]
                table = row[2]
                if (schema, table) not in tables:
                    tables[(schema, table)] = []
                tables[(schema, table)].append(row)
            
            tagged_columns = []
            
            # Process each table and its columns
            for (schema, table), rows in tables.items():
                # Get data types for all columns in this table at once
                data_types = self.get_column_data_types(database, schema, table)
                
                # Process each tagged column
                for row in rows:
                    column_name = row[3]
                    tag_value = row[5]
                    
                    # Get data type from our cached values
                    data_type = data_types.get(column_name, "VARCHAR")
                    
                    tagged_columns.append({
                        'schema': schema,
                        'table': table,
                        'column': column_name,
                        'data_type': data_type,
                        'tag_value': tag_value
                    })
                    logger.info(f"Found tagged column: {schema}.{table}.{column_name} with tag '{tag_name}' value: {tag_value}")
            
            logger.info(f"Found {len(tagged_columns)} tagged columns with tag '{tag_name}' and specified categories")
            return tagged_columns
        except Exception as e:
            logger.error(f"Error retrieving tagged columns: {e}")
            return []
        finally:
            if cursor:
                cursor.close()

    def get_tagged_columns_direct(self, database: str, tag_name: str, categories: List[str]) -> List[Dict[str, str]]:
        """
        Alternative approach: Query system tables directly for tagged columns
        This bypasses ACCOUNT_USAGE and might pick up more recent changes
        """
        try:
            cursor = self.connector.conn.cursor()
            
            # Convert inputs for case-insensitive matching
            tag_name_upper = tag_name.upper()
            
            # Use direct system tables query instead of ACCOUNT_USAGE
            schemas_list = ", ".join([f"'{schema}'" for schema in self.list_schemas(database)])
            
            # Build category conditions
            category_conditions = []
            for cat in categories:
                # Use ILIKE for case-insensitive partial matching
                category_conditions.append(f"UPPER(t.value::string) ILIKE '%{cat.upper()}%'")
            
            category_filter = " OR ".join(category_conditions)
            
            # Direct query to system tables
            sql = f"""
            SELECT
                c.table_schema as schema_name,
                c.table_name,
                c.column_name,
                c.data_type,
                t.value::string as tag_value
            FROM
                {database}.INFORMATION_SCHEMA.COLUMNS c
            JOIN
                TABLE({database}.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('{tag_name}')) t
            ON
                t.object_database = '{database}'
                AND t.object_schema = c.table_schema
                AND t.object_name = c.table_name
                AND t.column_name = c.column_name
            WHERE
                c.table_schema IN ({schemas_list})
                AND ({category_filter})
            """
            
            logger.debug(f"Executing direct query: {sql}")
            cursor.execute(sql)
            results = cursor.fetchall()
            
            tagged_columns = []
            
            # Process results
            for row in results:
                schema = row[0]
                table = row[1]
                column = row[2]
                data_type = row[3]
                tag_value = row[4]
                
                tagged_columns.append({
                    'schema': schema,
                    'table': table,
                    'column': column,
                    'data_type': data_type,
                    'tag_value': tag_value
                })
                logger.info(f"Direct query: Found tagged column: {schema}.{table}.{column} with tag '{tag_name}' value: {tag_value}")
            
            logger.info(f"Direct query: Found {len(tagged_columns)} tagged columns with tag '{tag_name}' and specified categories")
            return tagged_columns
        except Exception as e:
            logger.error(f"Error in direct query for tagged columns: {e}")
            return []
        finally:
            if cursor:
                cursor.close()

    def list_schemas(self, database: str) -> List[str]:
        """List all schemas in the database"""
        try:
            cursor = self.connector.conn.cursor()
            sql = f"SHOW SCHEMAS IN DATABASE {database}"
            cursor.execute(sql)
            schemas = [row[1] for row in cursor.fetchall()]
            return schemas
        except Exception as e:
            logger.error(f"Error listing schemas: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
    
    def get_column_data_types(self, database: str, schema: str, table: str) -> Dict[str, str]:
        """Get data types for all columns in a table"""
        data_types = {}
        try:
            cursor = self.connector.conn.cursor()
            sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM {database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            """
            cursor.execute(sql)
            for row in cursor.fetchall():
                data_types[row[0]] = row[1]
            return data_types
        except Exception as e:
            logger.error(f"Error getting column data types: {e}")
            return {}
        finally:
            if cursor:
                cursor.close()
    
    def set_active_schema(self, schema: str) -> None:
        """Set the active schema for policy operations"""
        self._active_schema = schema
    
    def get_active_schema(self, database: str = None) -> str:
        """Get the currently active schema or detect from database"""
        if self._active_schema:
            return self._active_schema
        
        # If no active schema is set, try to detect the current schema
        try:
            cursor = self.connector.conn.cursor()
            cursor.execute("SELECT CURRENT_SCHEMA()")
            current_schema = cursor.fetchone()[0]
            self._active_schema = current_schema
            return current_schema
        except Exception as e:
            logger.error(f"Error detecting current schema: {e}")
            # Default to PUBLIC if detection fails
            return "PUBLIC"
        finally:
            if cursor:
                cursor.close()
    
    def get_tables_with_tagged_columns(self, database: str, tag_name: str, categories: List[str]) -> List[Dict[str, str]]:
        """Get all tables that have columns with the specified tag categories"""
        tagged_columns = self.get_tagged_columns(database, tag_name, categories)
        
        # Extract unique tables
        tables = {}
        for col in tagged_columns:
            key = (col['schema'], col['table'])
            if key not in tables:
                tables[key] = {
                    'schema': col['schema'],
                    'table': col['table']
                }
        
        return list(tables.values())
    
    def create_row_access_policy(self, policy: Dict[str, str]) -> bool:
        """Create a row access policy"""
        try:
            cursor = self.connector.conn.cursor()
            
            name = policy.get('name')
            database = policy.get('database')
            policy_schema = policy.get('policy_schema')
            policy_expr = policy.get('policy_expression')
            comment = policy.get('comment', '')
            
            if not policy_schema:
                policy_schema = self.get_active_schema(database)
            
            # Check if the policy already exists
            sql = f"""
            SHOW ROW ACCESS POLICIES IN SCHEMA {database}.{policy_schema}
            """
            cursor.execute(sql)
            policies = cursor.fetchall()
            existing = False
            for row in policies:
                if row[1].upper() == name.upper():
                    existing = True
                    break
            
            if existing:
                logger.info(f"Row access policy {name} already exists, updating")
                sql = f"""
                ALTER ROW ACCESS POLICY {database}.{policy_schema}.{name} 
                SET BODY -> {policy_expr}
                """
            else:
                logger.info(f"Creating new row access policy: {name}")
                sql = f"""
                CREATE OR REPLACE ROW ACCESS POLICY {database}.{policy_schema}.{name}
                AS (ROW {policy_expr})
                """
                if comment:
                    sql += f" COMMENT = '{comment}'"
            
            logger.debug(f"Executing SQL: {sql}")
            cursor.execute(sql)
            return True
        except Exception as e:
            logger.error(f"Error creating row access policy: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def apply_row_access_policy(self, policy: Dict[str, str]) -> bool:
        """Apply a row access policy to a table"""
        try:
            cursor = self.connector.conn.cursor()
            
            name = policy.get('name')
            database = policy.get('database')
            schema = policy.get('schema')
            table = policy.get('table')
            policy_schema = policy.get('policy_schema')
            
            if not policy_schema:
                policy_schema = schema
            
            sql = f"""
            ALTER TABLE {database}.{schema}.{table}
            ADD ROW ACCESS POLICY {database}.{policy_schema}.{name}
            ON ({database},{schema},{table})
            """
            
            logger.debug(f"Applying row access policy: {sql}")
            cursor.execute(sql)
            logger.info(f"Successfully applied row access policy {name} to table {schema}.{table}")
            return True
        except Exception as e:
            logger.error(f"Error applying row access policy: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def create_category_masking_policy(self, database: str, schema: str, name: str, 
                                      data_type: str, expression: str, comment: str = '') -> bool:
        """Create a masking policy for a specific data type and category"""
        try:
            cursor = self.connector.conn.cursor()
            
            # Fix: Changed how we check if the policy exists
            policy_name = f"{name}_{data_type}"
            
            # Check if the policy already exists using SHOW without LIKE
            sql = f"""
            SHOW MASKING POLICIES IN SCHEMA {database}.{schema}
            """
            cursor.execute(sql)
            policies = cursor.fetchall()
            existing = False
            for row in policies:
                if row[1].upper() == policy_name.upper():
                    existing = True
                    break
            
            if existing:
                logger.info(f"Masking policy {policy_name} already exists, updating")
                sql = f"""
                ALTER MASKING POLICY {database}.{schema}.{policy_name} 
                SET BODY -> {expression}
                """
            else:
                logger.info(f"Creating new masking policy: {policy_name}")
                sql = f"""
                CREATE OR REPLACE MASKING POLICY {database}.{schema}.{policy_name}
                AS (val {data_type}) RETURNS {data_type} -> {expression}
                """
                if comment:
                    sql += f" COMMENT = '{comment}'"
            
            logger.info(f"Executing SQL: {sql}")
            cursor.execute(sql)
            return True
        except Exception as e:
            logger.error(f"Error creating masking policy: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def apply_masking_policy_by_data_type(self, database: str, policy_schema: str, policy_name: str,
                                        schema: str, table: str, column: str, data_type: str) -> bool:
        """Apply a masking policy to a column based on its data type"""
        try:
            cursor = self.connector.conn.cursor()
            
            # Full policy name includes the data type
            full_policy_name = f"{policy_name}_{data_type}"
            
            # Apply the policy to the column
            sql = f"""
            ALTER TABLE {database}.{schema}.{table} MODIFY COLUMN {column}
            SET MASKING POLICY {database}.{policy_schema}.{full_policy_name}
            """
            
            logger.debug(f"Applying masking policy: {sql}")
            cursor.execute(sql)
            logger.info(f"Successfully applied masking policy {full_policy_name} to column {schema}.{table}.{column}")
            return True
        except Exception as e:
            logger.error(f"Error applying masking policy: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def list_available_tags(self, database: str) -> List[str]:
        """List all available tags in the database"""
        try:
            cursor = self.connector.conn.cursor()
            
            # Query to get all unique tag names
            sql = f"""
            SELECT DISTINCT TAG_NAME 
            FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
            WHERE OBJECT_DATABASE = '{database}'
            """
            
            cursor.execute(sql)
            tags = [row[0] for row in cursor.fetchall()]
            return tags
        except Exception as e:
            logger.error(f"Error listing available tags: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
    
    def run_pii_detection(self, rules: List[Dict[str, Any]], auto_tagging: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Run PII detection on database tables"""
        # This is a placeholder. In a real implementation, this would scan tables
        # and detect PII based on patterns and rules.
        from .pii_detector import PIIDetector
        
        findings = {}
        # Implementation would go here
        
        return findings
    
    def apply_pii_tags(self, findings: Dict[str, List[Dict[str, Any]]], auto_tagging: Dict[str, Any]) -> int:
        """Apply PII tags to columns based on detection findings"""
        # This is a placeholder. In a real implementation, this would apply
        # tags to columns based on the findings.
        tagged_count = 0
        # Implementation would go here
        
        return tagged_count